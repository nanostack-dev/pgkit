package adminui

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nanostack-dev/pgkit/workflow"
)

type workflowFixture struct {
	server   string
	root     string
	approval string
	refusing string
	failedAt time.Time
}

func startWorkflowFixture(t *testing.T) workflowFixture {
	t.Helper()
	ctx := context.Background()
	db := createTestDB(t, ctx)
	queue, workflows := newClients(t, ctx, db)
	approved := workflow.NewSignal[string]("approved")
	leaf := workflow.Define("adminui-leaf", func(wf *workflow.Context, n int) (int, error) {
		return wf.Step("work", func(context.Context) (int, error) { return n + 1, nil })
	})
	branch := workflow.Define("adminui-branch", func(wf *workflow.Context, n int) (int, error) {
		return wf.Call("leaf", leaf, n)
	})
	root := workflow.Define("adminui-root", func(wf *workflow.Context, n int) (int, error) {
		first, err := wf.Call("branch", branch, n)
		if err != nil {
			return 0, err
		}
		second, err := wf.Call("leaf", leaf, first)
		if err != nil {
			return 0, err
		}
		return wf.Step("finalize", func(context.Context) (int, error) { return second, nil })
	})
	approval := workflow.Define("adminui-approval", func(wf *workflow.Context, _ struct{}) (string, error) {
		return wf.Receive(approved, workflow.Forever)
	})
	refusing := workflow.Define("adminui-refusing", func(*workflow.Context, struct{}) (string, error) {
		return "", errors.New("refused")
	})
	runWorker(t, ctx, workflows, root, branch, leaf, approval, refusing)

	rootRun, err := workflows.Start(ctx, root, 20, workflow.Key("100%-root"))
	if err != nil {
		t.Fatal(err)
	}
	approvalRun, err := workflows.Start(ctx, approval, struct{}{}, workflow.Key("1000-approval"))
	if err != nil {
		t.Fatal(err)
	}
	refusingRun, err := workflows.Start(ctx, refusing, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if got, err := rootRun.Result(waitCtx); err != nil || got != 22 {
		t.Fatalf("root result = %d, err = %v", got, err)
	}
	var runErr *workflow.RunError
	if _, err := refusingRun.Result(waitCtx); !errors.As(err, &runErr) {
		t.Fatalf("refusing result err = %v", err)
	}
	requireEventually(t, 15*time.Second, 20*time.Millisecond, func() bool {
		info, err := workflows.GetRun(ctx, approvalRun.ID)
		return err == nil && info.Status == workflow.RunWaiting
	})
	failed, err := workflows.GetRun(ctx, refusingRun.ID)
	if err != nil {
		t.Fatal(err)
	}
	server := newServer(t, queue, Options{Token: "secret", Workflow: workflows})
	return workflowFixture{server: server.URL, root: rootRun.ID, approval: approvalRun.ID, refusing: refusingRun.ID, failedAt: failed.CompletedAt}
}

func findRun(t *testing.T, runs []runSummary, id string) runSummary {
	t.Helper()
	for _, run := range runs {
		if run.ID == id {
			return run
		}
	}
	t.Fatalf("run %s not in %d listed runs", id, len(runs))
	return runSummary{}
}

func runIDs(runs []runSummary) []string {
	ids := make([]string, 0, len(runs))
	for _, run := range runs {
		ids = append(ids, run.ID)
	}
	slices.Sort(ids)
	return ids
}

func TestWorkflowReadModels(t *testing.T) {
	fixture := startWorkflowFixture(t)
	runsURL := fixture.server + "/api/dashboard/workflow/runs"

	t.Run("workflow summaries", func(t *testing.T) {
		var response struct {
			Items []workflowSummary `json:"items"`
		}
		getJSON(t, fixture.server+"/api/dashboard/workflow/workflows", http.StatusOK, &response)
		names := make([]string, 0, len(response.Items))
		byName := map[string]workflowSummary{}
		for _, item := range response.Items {
			names = append(names, item.Workflow)
			byName[item.Workflow] = item
		}
		want := []string{"adminui-approval", "adminui-branch", "adminui-leaf", "adminui-refusing", "adminui-root"}
		if !slices.Equal(names, want) {
			t.Fatalf("workflows = %v, want %v", names, want)
		}
		if leaf := byName["adminui-leaf"]; leaf.Total != 2 || leaf.Succeeded != 2 || !slices.Equal(leaf.Versions, []int{0}) || leaf.LastRunAt == "" {
			t.Fatalf("leaf = %+v", leaf)
		}
		if approval := byName["adminui-approval"]; approval.Total != 1 || approval.Waiting != 1 || approval.Succeeded != 0 {
			t.Fatalf("approval = %+v", approval)
		}
		if refusing := byName["adminui-refusing"]; refusing.Total != 1 || refusing.Failed != 1 {
			t.Fatalf("refusing = %+v", refusing)
		}
		raw := getMap(t, fixture.server+"/api/dashboard/workflow/workflows", http.StatusOK)
		requireKeys(t, "workflows response", raw, "items")
		requireKeys(t, "workflow item", raw["items"].([]any)[0].(map[string]any), "workflow", "versions", "total", "pending", "running", "waiting", "succeeded", "failed", "cancelled", "last_run_at")
	})

	t.Run("run list hides children of top level runs", func(t *testing.T) {
		var all listResponse[runSummary]
		getJSON(t, runsURL, http.StatusOK, &all)
		if all.Total != 6 || len(all.Items) != 6 || all.Limit != defaultListLimit || all.Offset != 0 {
			t.Fatalf("all = total %d items %d limit %d offset %d", all.Total, len(all.Items), all.Limit, all.Offset)
		}
		for i := 1; i < len(all.Items); i++ {
			if mustParseTime(t, all.Items[i].CreatedAt).After(mustParseTime(t, all.Items[i-1].CreatedAt)) {
				t.Fatalf("runs are not newest first: %s after %s", all.Items[i].CreatedAt, all.Items[i-1].CreatedAt)
			}
		}

		var top listResponse[runSummary]
		getJSON(t, runsURL+"?top_level=true", http.StatusOK, &top)
		want := []string{fixture.root, fixture.approval, fixture.refusing}
		slices.Sort(want)
		if top.Total != 3 || !slices.Equal(runIDs(top.Items), want) {
			t.Fatalf("top level = total %d ids %v, want %v", top.Total, runIDs(top.Items), want)
		}
		for _, run := range top.Items {
			if run.ParentRunID != nil {
				t.Fatalf("top level run %s has parent %s", run.ID, *run.ParentRunID)
			}
		}

		var children listResponse[runSummary]
		getJSON(t, runsURL+"?parent_run_id="+fixture.root, http.StatusOK, &children)
		if children.Total != 2 || len(children.Items) != 2 {
			t.Fatalf("children = %+v", children)
		}
		for _, child := range children.Items {
			if child.ParentRunID == nil || *child.ParentRunID != fixture.root {
				t.Fatalf("child = %+v", child)
			}
		}
	})

	t.Run("run summaries carry counts and the current step", func(t *testing.T) {
		var all listResponse[runSummary]
		getJSON(t, runsURL, http.StatusOK, &all)

		root := findRun(t, all.Items, fixture.root)
		if root.Status != "succeeded" || root.Key == nil || *root.Key != "100%-root" || root.ChildCount != 2 || root.StepCount != 3 || root.CurrentStep != nil {
			t.Fatalf("root = %+v", root)
		}
		if root.Version != 0 || root.Workflow != "adminui-root" || root.CompletedAt == nil || root.StartedAt == nil || root.Error != nil || root.WakeAt != nil {
			t.Fatalf("root = %+v", root)
		}
		approval := findRun(t, all.Items, fixture.approval)
		step := approval.CurrentStep
		if approval.Status != "waiting" || approval.StepCount != 1 || approval.ChildCount != 0 || step == nil {
			t.Fatalf("approval = %+v", approval)
		}
		if step.Name != "approved" || step.Kind != "signal" || step.Status != "waiting" || step.Signal == nil || *step.Signal != "approved" || step.WakeAt != nil || step.ChildRunID != nil || step.Error != nil {
			t.Fatalf("approval step = %+v", step)
		}
		refusing := findRun(t, all.Items, fixture.refusing)
		if refusing.Status != "failed" || refusing.Error == nil || *refusing.Error != "refused" || refusing.StepCount != 0 || refusing.CurrentStep != nil {
			t.Fatalf("refusing = %+v", refusing)
		}

		raw := getMap(t, runsURL, http.StatusOK)
		requireKeys(t, "runs response", raw, "items", "total", "limit", "offset")
		for _, item := range raw["items"].([]any) {
			requireKeys(t, "run summary", item.(map[string]any), "id", "workflow", "version", "key", "status", "error", "timed_out", "parent_run_id", "wake_at", "deadline_at", "created_at", "started_at", "completed_at", "updated_at", "child_count", "step_count", "current_step")
		}
		for _, item := range raw["items"].([]any) {
			summary := item.(map[string]any)
			if step, ok := summary["current_step"].(map[string]any); ok {
				requireKeys(t, "current step", step, "name", "kind", "status", "attempts", "signal", "wake_at", "child_run_id", "error")
			}
		}
	})

	t.Run("run list filters and paging", func(t *testing.T) {
		var waiting listResponse[runSummary]
		getJSON(t, runsURL+"?status=waiting", http.StatusOK, &waiting)
		if waiting.Total != 1 || waiting.Items[0].ID != fixture.approval {
			t.Fatalf("waiting = %+v", waiting)
		}
		var leaves listResponse[runSummary]
		getJSON(t, runsURL+"?workflow=adminui-leaf", http.StatusOK, &leaves)
		if leaves.Total != 2 {
			t.Fatalf("leaves = %+v", leaves)
		}
		var page listResponse[runSummary]
		getJSON(t, runsURL+"?limit=2&offset=2", http.StatusOK, &page)
		if page.Total != 6 || len(page.Items) != 2 || page.Limit != 2 || page.Offset != 2 {
			t.Fatalf("page = total %d items %d limit %d offset %d", page.Total, len(page.Items), page.Limit, page.Offset)
		}
		var past listResponse[runSummary]
		getJSON(t, runsURL+"?limit=2&offset=50", http.StatusOK, &past)
		if past.Total != 6 || past.Items == nil || len(past.Items) != 0 {
			t.Fatalf("past = %+v", past)
		}
	})

	t.Run("search treats wildcards literally", func(t *testing.T) {
		search := func(term string) listResponse[runSummary] {
			var response listResponse[runSummary]
			getJSON(t, runsURL+"?search="+url.QueryEscape(term), http.StatusOK, &response)
			return response
		}
		if percent := search("%"); percent.Total != 1 || percent.Items[0].ID != fixture.root {
			t.Fatalf("search %% = %+v", percent)
		}
		if literal := search("100%"); literal.Total != 1 || literal.Items[0].ID != fixture.root {
			t.Fatalf("search 100%% = %+v", literal)
		}
		if underscore := search("_"); underscore.Total != 0 {
			t.Fatalf("search _ = %+v", underscore)
		}
		if backslash := search(`\`); backslash.Total != 0 {
			t.Fatalf("search backslash = %+v", backslash)
		}
		if byKey := search("1000-APPR"); byKey.Total != 1 || byKey.Items[0].ID != fixture.approval {
			t.Fatalf("search by key = %+v", byKey)
		}
		if byWorkflow := search("adminui-refusing"); byWorkflow.Total != 1 || byWorkflow.Items[0].ID != fixture.refusing {
			t.Fatalf("search by workflow = %+v", byWorkflow)
		}
		if byID := search(fixture.approval[:13]); byID.Total != 1 || byID.Items[0].ID != fixture.approval {
			t.Fatalf("search by id = %+v", byID)
		}
		injection := search("'; DROP TABLE pgworkflow_runs; --")
		if injection.Total != 0 {
			t.Fatalf("search injection = %+v", injection)
		}
		var all listResponse[runSummary]
		getJSON(t, runsURL, http.StatusOK, &all)
		if all.Total != 6 {
			t.Fatalf("runs after injection attempt = %d", all.Total)
		}
	})

	t.Run("run detail", func(t *testing.T) {
		var detail workflowRunDetail
		getJSON(t, runsURL+"/"+fixture.root, http.StatusOK, &detail)
		if detail.Run.ID != fixture.root || detail.Run.Input != "20" || detail.Run.Output == nil || *detail.Run.Output != "22" {
			t.Fatalf("run = %+v", detail.Run)
		}
		if _, err := time.Parse(time.RFC3339Nano, detail.Now); err != nil {
			t.Fatalf("now = %q: %v", detail.Now, err)
		}
		if detail.JobID == nil || detail.ChildTotal != 2 || len(detail.Children) != 2 {
			t.Fatalf("job_id=%v child_total=%d children=%d", detail.JobID, detail.ChildTotal, len(detail.Children))
		}
		if detail.Children[0].Workflow != "adminui-branch" || detail.Children[1].Workflow != "adminui-leaf" {
			t.Fatalf("children are not oldest first: %s, %s", detail.Children[0].Workflow, detail.Children[1].Workflow)
		}
		if detail.Children[0].ChildCount != 1 || detail.Children[0].StepCount != 1 || detail.Children[1].ChildCount != 0 || detail.Children[1].StepCount != 1 {
			t.Fatalf("children = %+v", detail.Children)
		}
		names := make([]string, 0, len(detail.Steps))
		for _, step := range detail.Steps {
			names = append(names, step.Name)
		}
		if !slices.Equal(names, []string{"branch", "leaf", "finalize"}) {
			t.Fatalf("steps = %v", names)
		}
		if detail.Ancestors == nil || len(detail.Ancestors) != 0 || detail.Signals == nil || len(detail.Signals) != 0 {
			t.Fatalf("ancestors = %v signals = %v", detail.Ancestors, detail.Signals)
		}

		raw := getMap(t, runsURL+"/"+fixture.root, http.StatusOK)
		requireKeys(t, "run detail", raw, "now", "run", "job_id", "steps", "children", "child_total", "ancestors", "signals")
		requireKeys(t, "run", raw["run"].(map[string]any), "id", "workflow", "version", "key", "status", "error", "timed_out", "parent_run_id", "input", "output", "wake_at", "deadline_at", "created_at", "started_at", "completed_at", "updated_at")

		var job queueJobDetail
		getJSON(t, fixture.server+"/api/dashboard/queue/jobs/"+strconv.FormatInt(*detail.JobID, 10), http.StatusOK, &job)
		if job.RunID == nil || *job.RunID != fixture.root {
			t.Fatalf("job for run = %+v", job)
		}
		requireErrorBody(t, runsURL+"/missing", http.StatusNotFound, "run not found")
	})

	t.Run("run detail lists ancestors root first", func(t *testing.T) {
		var branches listResponse[runSummary]
		getJSON(t, runsURL+"?workflow=adminui-branch", http.StatusOK, &branches)
		var grandchildren listResponse[runSummary]
		getJSON(t, runsURL+"?parent_run_id="+branches.Items[0].ID, http.StatusOK, &grandchildren)
		if grandchildren.Total != 1 {
			t.Fatalf("grandchildren = %+v", grandchildren)
		}
		var detail workflowRunDetail
		getJSON(t, runsURL+"/"+grandchildren.Items[0].ID, http.StatusOK, &detail)
		if len(detail.Ancestors) != 2 {
			t.Fatalf("ancestors = %+v", detail.Ancestors)
		}
		first, second := detail.Ancestors[0], detail.Ancestors[1]
		if first.ID != fixture.root || first.Workflow != "adminui-root" || first.Status != "succeeded" || first.Key == nil || *first.Key != "100%-root" {
			t.Fatalf("first ancestor = %+v", first)
		}
		if second.ID != branches.Items[0].ID || second.Workflow != "adminui-branch" || second.Key == nil || *second.Key != fixture.root+"/branch" {
			t.Fatalf("second ancestor = %+v", second)
		}
		raw := getMap(t, runsURL+"/"+grandchildren.Items[0].ID, http.StatusOK)
		requireKeys(t, "ancestor", raw["ancestors"].([]any)[0].(map[string]any), "id", "workflow", "status", "key")
		if detail.ChildTotal != 0 || len(detail.Children) != 0 {
			t.Fatalf("grandchild children = %d/%d", detail.ChildTotal, len(detail.Children))
		}
	})

	t.Run("run tree is depth first in creation order", func(t *testing.T) {
		var tree runTree
		getJSON(t, runsURL+"/"+fixture.root+"/tree", http.StatusOK, &tree)
		if tree.RootID != fixture.root || tree.Truncated || len(tree.Nodes) != 4 {
			t.Fatalf("tree = %+v", tree)
		}
		workflows := make([]string, 0, len(tree.Nodes))
		depths := make([]int, 0, len(tree.Nodes))
		counts := make([]int, 0, len(tree.Nodes))
		for _, node := range tree.Nodes {
			workflows = append(workflows, node.Workflow)
			depths = append(depths, node.Depth)
			counts = append(counts, node.ChildCount)
		}
		if !slices.Equal(workflows, []string{"adminui-root", "adminui-branch", "adminui-leaf", "adminui-leaf"}) {
			t.Fatalf("workflows = %v", workflows)
		}
		if !slices.Equal(depths, []int{0, 1, 2, 1}) || !slices.Equal(counts, []int{2, 1, 0, 0}) {
			t.Fatalf("depths = %v child counts = %v", depths, counts)
		}
		if tree.Nodes[0].ParentRunID != nil || tree.Nodes[1].ParentRunID == nil || *tree.Nodes[1].ParentRunID != fixture.root || *tree.Nodes[2].ParentRunID != tree.Nodes[1].ID || *tree.Nodes[3].ParentRunID != fixture.root {
			t.Fatalf("parents = %+v", tree.Nodes)
		}
		if tree.Nodes[0].Key == nil || *tree.Nodes[0].Key != "100%-root" || tree.Nodes[0].Status != "succeeded" || tree.Nodes[0].CompletedAt == nil {
			t.Fatalf("root node = %+v", tree.Nodes[0])
		}

		var subtree runTree
		getJSON(t, runsURL+"/"+tree.Nodes[1].ID+"/tree", http.StatusOK, &subtree)
		if subtree.RootID != tree.Nodes[1].ID || len(subtree.Nodes) != 2 || subtree.Nodes[0].Depth != 0 || subtree.Nodes[1].Depth != 1 || subtree.Truncated {
			t.Fatalf("subtree = %+v", subtree)
		}
		var single runTree
		getJSON(t, runsURL+"/"+fixture.refusing+"/tree", http.StatusOK, &single)
		if len(single.Nodes) != 1 || single.Nodes[0].ChildCount != 0 || single.Truncated {
			t.Fatalf("single = %+v", single)
		}

		raw := getMap(t, runsURL+"/"+fixture.root+"/tree", http.StatusOK)
		requireKeys(t, "tree", raw, "root_id", "nodes", "truncated")
		requireKeys(t, "tree node", raw["nodes"].([]any)[0].(map[string]any), "id", "parent_run_id", "workflow", "version", "key", "status", "depth", "created_at", "completed_at", "child_count")
		requireErrorBody(t, runsURL+"/missing/tree", http.StatusNotFound, "run not found")
	})

	t.Run("workflow overview", func(t *testing.T) {
		var overview overviewResponse
		getJSON(t, fixture.server+"/api/dashboard/overview", http.StatusOK, &overview)
		section := overview.Workflow
		if section == nil {
			t.Fatal("workflow section is null")
		}
		wantCounts := workflowCounts{Waiting: 1, Succeeded: 4, Failed: 1, Total: 6}
		if section.Counts != wantCounts {
			t.Fatalf("counts = %+v, want %+v", section.Counts, wantCounts)
		}
		if len(section.Waiting) != 1 || section.Waiting[0].ID != fixture.approval {
			t.Fatalf("waiting = %+v", section.Waiting)
		}
		step := section.Waiting[0].CurrentStep
		if step == nil || step.Kind != "signal" || step.Signal == nil || *step.Signal != "approved" {
			t.Fatalf("waiting step = %+v", step)
		}
		if len(section.Failed) != 1 || section.Failed[0].ID != fixture.refusing || section.Failed[0].Error == nil || *section.Failed[0].Error != "refused" {
			t.Fatalf("failed = %+v", section.Failed)
		}
		if len(section.Recent) != 6 {
			t.Fatalf("recent = %d runs", len(section.Recent))
		}
		for i := 1; i < len(section.Recent); i++ {
			if mustParseTime(t, section.Recent[i].UpdatedAt).After(mustParseTime(t, section.Recent[i-1].UpdatedAt)) {
				t.Fatalf("recent runs are not newest first")
			}
		}

		starts := make([]string, 0, len(section.Throughput.Buckets))
		var succeeded, failed, cancelled int64
		for _, bucket := range section.Throughput.Buckets {
			starts = append(starts, bucket.Start)
			succeeded += bucket.Succeeded
			failed += bucket.Failed
			cancelled += bucket.Cancelled
		}
		requireBucketLayout(t, starts, overview.Now, 60, 60)
		if section.Throughput.BucketSeconds != 60 || succeeded != 4 || failed != 1 || cancelled != 0 {
			t.Fatalf("throughput = %d s, %d succeeded, %d failed, %d cancelled", section.Throughput.BucketSeconds, succeeded, failed, cancelled)
		}
		if index := bucketContaining(t, starts, 60, fixture.failedAt); section.Throughput.Buckets[index].Failed != 1 {
			t.Fatalf("failed run is not in the bucket of its completion: %+v", section.Throughput.Buckets[index])
		}

		var daily overviewResponse
		getJSON(t, fixture.server+"/api/dashboard/overview?range=24h", http.StatusOK, &daily)
		dailyStarts := make([]string, 0, 48)
		for _, bucket := range daily.Workflow.Throughput.Buckets {
			dailyStarts = append(dailyStarts, bucket.Start)
		}
		requireBucketLayout(t, dailyStarts, daily.Now, 1800, 48)
		if daily.Workflow.Throughput.BucketSeconds != 1800 {
			t.Fatalf("bucket_seconds = %d", daily.Workflow.Throughput.BucketSeconds)
		}

		raw := getMap(t, fixture.server+"/api/dashboard/overview", http.StatusOK)
		workflowSection := raw["workflow"].(map[string]any)
		requireKeys(t, "workflow overview", workflowSection, "counts", "throughput", "waiting", "failed", "recent")
		requireKeys(t, "counts", workflowSection["counts"].(map[string]any), "pending", "running", "waiting", "succeeded", "failed", "cancelled", "total")
		throughput := workflowSection["throughput"].(map[string]any)
		requireKeys(t, "throughput", throughput, "bucket_seconds", "buckets")
		requireKeys(t, "bucket", throughput["buckets"].([]any)[0].(map[string]any), "start", "succeeded", "failed", "cancelled")
	})
}

func TestWorkflowRunDetailShowsSignals(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)
	queue, workflows := newClients(t, ctx, db)
	approved := workflow.NewSignal[string]("approved")
	noise := workflow.NewSignal[string]("noise")
	approval := workflow.Define("adminui-signals", func(wf *workflow.Context, _ struct{}) (string, error) {
		return wf.Receive(approved, workflow.Forever)
	})
	runWorker(t, ctx, workflows, approval)
	run, err := workflows.Start(ctx, approval, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	requireEventually(t, 15*time.Second, 20*time.Millisecond, func() bool {
		info, err := workflows.GetRun(ctx, run.ID)
		return err == nil && info.Status == workflow.RunWaiting
	})
	server := newServer(t, queue, Options{Token: "secret", Workflow: workflows})
	detailURL := server.URL + "/api/dashboard/workflow/runs/" + run.ID

	if err := workflows.Signal(ctx, run.ID, noise, "ignored"); err != nil {
		t.Fatal(err)
	}
	var waiting workflowRunDetail
	getJSON(t, detailURL, http.StatusOK, &waiting)
	if len(waiting.Signals) != 1 {
		t.Fatalf("signals = %+v", waiting.Signals)
	}
	if sent := waiting.Signals[0]; sent.Name != "noise" || sent.Received || sent.PayloadPreview != `"ignored"` || sent.ID == 0 {
		t.Fatalf("signal = %+v", sent)
	}
	if waiting.JobID == nil || waiting.Run.Status != "waiting" {
		t.Fatalf("detail = job %v status %s", waiting.JobID, waiting.Run.Status)
	}

	long := strings.Repeat("y", 300)
	if err := workflows.Signal(ctx, run.ID, approved, long); err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if got, err := run.Result(waitCtx); err != nil || got != long {
		t.Fatalf("result err = %v", err)
	}
	var finished workflowRunDetail
	getJSON(t, detailURL, http.StatusOK, &finished)
	if len(finished.Signals) != 2 {
		t.Fatalf("signals = %+v", finished.Signals)
	}
	newest, oldest := finished.Signals[0], finished.Signals[1]
	if newest.Name != "approved" || !newest.Received || newest.ID <= oldest.ID {
		t.Fatalf("newest signal = %+v", newest)
	}
	if !strings.HasSuffix(newest.PayloadPreview, "…") || len(newest.PayloadPreview) != signalPreviewBytes+len("…") || !strings.HasPrefix(newest.PayloadPreview, `"yyy`) {
		t.Fatalf("preview = %q (%d bytes)", newest.PayloadPreview, len(newest.PayloadPreview))
	}
	if oldest.Name != "noise" || oldest.Received {
		t.Fatalf("oldest signal = %+v", oldest)
	}
	raw := getMap(t, detailURL, http.StatusOK)
	requireKeys(t, "signal", raw["signals"].([]any)[0].(map[string]any), "id", "name", "received", "payload_preview", "created_at")
}

func TestWorkflowTreeDepthLimit(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)
	queue, workflows := newClients(t, ctx, db)
	var chain *workflow.Workflow[int, int]
	chain = workflow.Define("adminui-chain", func(wf *workflow.Context, remaining int) (int, error) {
		if remaining == 0 {
			return 0, nil
		}
		return wf.Call("next", chain, remaining-1)
	})
	runWorker(t, ctx, workflows, chain)
	run, err := workflows.Start(ctx, chain, 10)
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	if _, err := run.Result(waitCtx); err != nil {
		t.Fatal(err)
	}
	server := newServer(t, queue, Options{Token: "secret", Workflow: workflows})
	treeURL := func(id string) string { return server.URL + "/api/dashboard/workflow/runs/" + id + "/tree" }

	var tree runTree
	getJSON(t, treeURL(run.ID), http.StatusOK, &tree)
	if !tree.Truncated || len(tree.Nodes) != maxTreeDepth+1 {
		t.Fatalf("truncated=%v nodes=%d, want truncated with %d nodes", tree.Truncated, len(tree.Nodes), maxTreeDepth+1)
	}
	for depth, node := range tree.Nodes {
		if node.Depth != depth {
			t.Fatalf("node %d has depth %d", depth, node.Depth)
		}
	}
	if deepest := tree.Nodes[maxTreeDepth]; deepest.ChildCount != 1 {
		t.Fatalf("deepest node = %+v", deepest)
	}

	var shallower runTree
	getJSON(t, treeURL(tree.Nodes[5].ID), http.StatusOK, &shallower)
	if shallower.Truncated || len(shallower.Nodes) != 6 {
		t.Fatalf("shallower tree: truncated=%v nodes=%d", shallower.Truncated, len(shallower.Nodes))
	}

	var detail workflowRunDetail
	getJSON(t, server.URL+"/api/dashboard/workflow/runs/"+tree.Nodes[maxTreeDepth].ID, http.StatusOK, &detail)
	if len(detail.Ancestors) != maxTreeDepth || detail.Ancestors[0].ID != run.ID {
		t.Fatalf("ancestors = %d, first %v", len(detail.Ancestors), detail.Ancestors)
	}
}
