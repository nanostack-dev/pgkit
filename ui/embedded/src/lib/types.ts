export type QueueJobStatus = 'pending' | 'processing' | 'done' | 'failed';

export type WorkflowRunStatus = 'pending' | 'running' | 'waiting' | 'succeeded' | 'failed' | 'cancelled';

export type WorkflowStepKind = 'step' | 'sleep' | 'signal' | 'child';

export type WorkflowStepStatus = 'running' | 'waiting' | 'retrying' | 'succeeded' | 'failed' | 'timed_out';

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
	workflow: string;
	version: number;
	key: string | null;
	status: WorkflowRunStatus;
	error: string | null;
	timed_out: boolean;
	parent_run_id: string | null;
	input: string;
	output: string | null;
	wake_at: string | null;
	deadline_at: string | null;
	created_at: string;
	started_at: string | null;
	completed_at: string | null;
	updated_at: string;
};

export type WorkflowRunsResponse = {
	items: WorkflowRun[];
	total: number;
	limit: number;
	offset: number;
};

export type WorkflowStep = {
	name: string;
	kind: WorkflowStepKind;
	status: WorkflowStepStatus;
	attempts: number;
	output: string | null;
	error: string | null;
	wake_at: string | null;
	child_run_id: string | null;
	signal: string | null;
	created_at: string;
	completed_at: string | null;
	updated_at: string;
};

export type WorkflowRunDetail = {
	run: WorkflowRun;
	steps: WorkflowStep[];
	children: WorkflowRun[];
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
