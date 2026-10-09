export type JobStatus = 'pending' | 'processing' | 'done' | 'failed';
export type RunStatus = 'pending' | 'running' | 'waiting' | 'succeeded' | 'failed' | 'cancelled';
export type StepKind = 'step' | 'sleep' | 'signal' | 'child';
export type StepStatus = 'running' | 'waiting' | 'retrying' | 'succeeded' | 'failed' | 'timed_out';

export const jobStatuses: JobStatus[] = ['pending', 'processing', 'done', 'failed'];
export const runStatuses: RunStatus[] = ['pending', 'running', 'waiting', 'succeeded', 'failed', 'cancelled'];

export type Page<T> = {
	items: T[];
	total: number;
	limit: number;
	offset: number;
};

export type AdminConfig = {
	mutations_enabled: boolean;
	workflows_enabled: boolean;
	page_size: number;
};

export type QueueJob = {
	id: number;
	queue_name: string;
	status: JobStatus;
	attempts: number;
	max_attempts: number;
	claims: number;
	available_at: string;
	claimed_by: string | null;
	claimed_at: string | null;
	done_at: string | null;
	last_error: string | null;
	payload_preview: string;
	created_at: string;
	updated_at: string;
};

export type PayloadEncoding = 'json' | 'text' | 'base64';

export type QueueJobDetail = QueueJob & {
	payload: string;
	payload_encoding: PayloadEncoding;
	payload_bytes: number;
	payload_truncated: boolean;
	run_id: string | null;
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

export type QueueStats = {
	queue_name: string;
	total: number;
	pending: number;
	due: number;
	processing: number;
	done: number;
	failed: number;
	oldest_due_at: string | null;
	last_activity_at: string;
};

export type QueuesResponse = {
	now: string;
	items: QueueStats[];
};

export type StepRef = {
	name: string;
	kind: StepKind;
	status: StepStatus;
	attempts: number;
	signal: string | null;
	wake_at: string | null;
	child_run_id: string | null;
	error: string | null;
};

export type RunFields = {
	id: string;
	workflow: string;
	version: number;
	key: string | null;
	status: RunStatus;
	error: string | null;
	timed_out: boolean;
	parent_run_id: string | null;
	wake_at: string | null;
	deadline_at: string | null;
	created_at: string;
	started_at: string | null;
	completed_at: string | null;
	updated_at: string;
};

export type RunSummary = RunFields & {
	child_count: number;
	step_count: number;
	current_step: StepRef | null;
};

export type WorkflowRun = RunFields & {
	input: string;
	output: string | null;
};

export type WorkflowStep = {
	name: string;
	kind: StepKind;
	status: StepStatus;
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

export type RunAncestor = {
	id: string;
	workflow: string;
	status: RunStatus;
	key: string | null;
};

export type RunSignal = {
	id: number;
	name: string;
	received: boolean;
	payload_preview: string;
	created_at: string;
};

export type RunDetail = {
	now: string;
	run: WorkflowRun;
	job_id: number | null;
	steps: WorkflowStep[];
	children: RunSummary[];
	child_total: number;
	ancestors: RunAncestor[];
	signals: RunSignal[];
};

export type RunTreeNode = {
	id: string;
	parent_run_id: string | null;
	workflow: string;
	version: number;
	key: string | null;
	status: RunStatus;
	depth: number;
	created_at: string;
	completed_at: string | null;
	child_count: number;
};

export type RunTree = {
	root_id: string;
	nodes: RunTreeNode[];
	truncated: boolean;
};

export type WorkflowStats = {
	workflow: string;
	versions: number[];
	total: number;
	pending: number;
	running: number;
	waiting: number;
	succeeded: number;
	failed: number;
	cancelled: number;
	last_run_at: string;
};

export type JobBucket = { start: string; done: number; failed: number };
export type RunBucket = { start: string; succeeded: number; failed: number; cancelled: number };

export type Series<B> = {
	bucket_seconds: number;
	buckets: B[];
};

export type RunCounts = Record<RunStatus, number> & { total: number };

export type OverviewRange = '1h' | '24h';

export type Overview = {
	now: string;
	queue: {
		summary: QueueSummary;
		due_jobs: number;
		oldest_due_at: string | null;
		throughput: Series<JobBucket>;
		failed_jobs: QueueJob[];
		recent_jobs: QueueJob[];
	};
	workflow: {
		counts: RunCounts;
		throughput: Series<RunBucket>;
		waiting: RunSummary[];
		failed: RunSummary[];
		recent: RunSummary[];
	} | null;
};

export type AdvisoryLock = {
	pid: number;
	mode: string;
	granted: boolean;
	key: string | null;
	classid: number;
	objid: number;
	objsubid: number;
	application_name: string;
	state: string | null;
	xact_start: string | null;
	state_change: string | null;
	wait_event_type: string | null;
	wait_event: string | null;
};

export type LocksResponse = {
	now: string;
	items: AdvisoryLock[];
};

export type EnqueueRequest = {
	queue_name: string;
	payload: unknown;
	max_attempts: number;
	delay_seconds: number;
};

export type JobFilters = {
	queue?: string;
	status?: JobStatus;
	search?: string;
	limit: number;
	offset: number;
};

export type RunFilters = {
	workflow?: string;
	status?: RunStatus;
	search?: string;
	parent_run_id?: string;
	top_level?: boolean;
	limit: number;
	offset: number;
};
