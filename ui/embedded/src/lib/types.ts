export type QueueJobStatus = 'pending' | 'processing' | 'done' | 'failed';

export type WorkflowRunStatus = 'running' | 'succeeded' | 'failed' | 'cancelling' | 'cancelled';

export type WorkflowStepStatus =
	| 'pending'
	| 'queued'
	| 'running'
	| 'waiting_retry'
	| 'succeeded'
	| 'failed'
	| 'cancelled'
	| 'skipped';

export type QueueJob = {
	id: number;
	queue_name: string;
	status: QueueJobStatus;
	attempts: number;
	max_attempts: number;
	available_at: string;
	claimed_by: string | null;
	claimed_at: string | null;
	done_at: string | null;
	last_error: string | null;
	payload_preview: string;
	created_at: string;
	updated_at: string;
};

export type QueueLock = {
	pid: number;
	mode: string;
	granted: boolean;
	classid: number;
	objid: number;
	objsubid: number;
};

export type QueueSummary = {
	total_jobs: number;
	pending_jobs: number;
	processing_jobs: number;
	done_jobs: number;
	failed_jobs: number;
	advisory_locks: number;
	queues: number;
};

export type QueueJobsResponse = {
	items: QueueJob[];
	total: number;
	limit: number;
	offset: number;
};

export type WorkflowRun = {
	id: string;
	workflow_definition_id: number;
	workflow_name: string;
	workflow_version: number;
	status: WorkflowRunStatus;
	started_at: string;
	completed_at: string | null;
	created_by: string | null;
	correlation_key: string | null;
	created_at: string;
	updated_at: string;
	input_json: string;
	context_json: string;
};

export type WorkflowRunsResponse = {
	items: WorkflowRun[];
	total: number;
	limit: number;
	offset: number;
};

export type WorkflowGraphNode = {
	id: string;
	kind: string;
	label: string;
	queue?: string;
	max_attempts?: number;
	depends_on?: string[];
	metadata?: Record<string, unknown>;
};

export type WorkflowGraphEdge = {
	from: string;
	to: string;
};

export type WorkflowStepRecord = {
	id: number;
	run_id: string;
	step_name: string;
	item_key: string;
	step_kind: string;
	status: WorkflowStepStatus;
	attempt: number;
	max_attempts: number;
	queue_job_id?: number | null;
	available_at?: string | null;
	started_at?: string | null;
	completed_at?: string | null;
	dependency_json?: string[];
	input_json?: string;
	output_json?: string;
	error_json?: string;
	created_at: string;
	updated_at: string;
};

export type WorkflowNodeItemCounts = {
	total: number;
	pending: number;
	queued: number;
	running: number;
	waiting_retry: number;
	succeeded: number;
	failed: number;
	cancelled: number;
	skipped: number;
};

export type WorkflowRunGraphNode = {
	node: WorkflowGraphNode;
	status: WorkflowStepStatus;
	step?: WorkflowStepRecord | null;
	items?: WorkflowStepRecord[];
	item_counts: WorkflowNodeItemCounts;
};

export type WorkflowRunGraphSummary = {
	total_nodes: number;
	pending_nodes: number;
	queued_nodes: number;
	running_nodes: number;
	retrying_nodes: number;
	succeeded_nodes: number;
	failed_nodes: number;
	cancelled_nodes: number;
	skipped_nodes: number;
	total_items: number;
	failed_items: number;
};

export type WorkflowDefinition = {
	id: number;
	workflow_name: string;
	version: number;
	status: string;
	title: string;
	description: string | null;
	content_hash: string;
	created_at: string;
	activated_at: string | null;
};

export type WorkflowRunGraphView = {
	run: WorkflowRun;
	definition: WorkflowDefinition;
	graph: {
		name: string;
		version: number;
		title: string;
		description?: string;
		nodes: WorkflowGraphNode[];
		edges: WorkflowGraphEdge[];
	};
	nodes: WorkflowRunGraphNode[];
	edges: WorkflowGraphEdge[];
	summary: WorkflowRunGraphSummary;
};

export type DashboardSnapshot = {
	queue: {
		summary: QueueSummary;
		jobs: QueueJobsResponse;
		locks: QueueLock[];
	};
	workflow: {
		runs: WorkflowRunsResponse;
	};
};
