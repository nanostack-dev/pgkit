package adminui

import (
	"context"
	"net/http"
	"strings"

	qpkg "github.com/nanostack-dev/pgkit/queue"
)

type throughputWindow struct {
	bucketSeconds int
	bucketCount   int
}

var throughputWindows = map[string]throughputWindow{
	"":    {bucketSeconds: 60, bucketCount: 60},
	"1h":  {bucketSeconds: 60, bucketCount: 60},
	"24h": {bucketSeconds: 1800, bucketCount: 48},
}

type dashboardConfig struct {
	MutationsEnabled bool `json:"mutations_enabled"`
	WorkflowsEnabled bool `json:"workflows_enabled"`
	PageSize         int  `json:"page_size"`
}

type overviewResponse struct {
	Now      string            `json:"now"`
	Queue    queueOverview     `json:"queue"`
	Workflow *workflowOverview `json:"workflow"`
}

func (u *UI) handleConfig(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, dashboardConfig{
		MutationsEnabled: u.enableMutations,
		WorkflowsEnabled: u.workflow != nil,
		PageSize:         u.listLimit,
	})
}

func (u *UI) handleOverview(w http.ResponseWriter, r *http.Request) {
	window, ok := throughputWindows[strings.TrimSpace(r.URL.Query().Get("range"))]
	if !ok {
		writeError(w, http.StatusBadRequest, errInvalidRange)
		return
	}
	var response overviewResponse
	err := u.withSnapshot(r.Context(), func(q querier) error {
		var err error
		response, err = u.buildOverview(r.Context(), q, window)
		return err
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (u *UI) buildOverview(ctx context.Context, q querier, window throughputWindow) (overviewResponse, error) {
	var response overviewResponse
	var err error
	if response.Now, err = queryNow(ctx, q); err != nil {
		return overviewResponse{}, err
	}
	if response.Queue, err = queryQueueOverview(ctx, q, window); err != nil {
		return overviewResponse{}, err
	}
	if u.workflow != nil {
		if response.Workflow, err = queryWorkflowOverview(ctx, q, window); err != nil {
			return overviewResponse{}, err
		}
	}
	return response, nil
}

func queryQueueOverview(ctx context.Context, q querier, window throughputWindow) (queueOverview, error) {
	counts, err := queryQueueCounts(ctx, q)
	if err != nil {
		return queueOverview{}, err
	}
	overview := queueOverview{
		Summary:     counts.summary,
		DueJobs:     counts.dueJobs,
		OldestDueAt: formatNullTime(counts.oldestDueAt),
	}
	if overview.Throughput, err = queryQueueThroughput(ctx, q, window); err != nil {
		return queueOverview{}, err
	}
	if overview.FailedJobs, err = queryQueueJobPreviews(ctx, q, qpkg.StatusFailed, overviewFailedJobs); err != nil {
		return queueOverview{}, err
	}
	if overview.RecentJobs, err = queryQueueJobPreviews(ctx, q, "", overviewRecentJobs); err != nil {
		return queueOverview{}, err
	}
	return overview, nil
}
