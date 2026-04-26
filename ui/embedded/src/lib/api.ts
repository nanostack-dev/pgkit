import type {
	DashboardSnapshot,
	QueueJobsResponse,
	QueueLock,
	QueueSummary,
	WorkflowRunGraphView,
	WorkflowRunsResponse
} from './types';

type QueryValue = string | number | undefined | null;

function queryString(values: Record<string, QueryValue>): string {
	const params = new URLSearchParams();
	for (const [key, value] of Object.entries(values)) {
		if (value === undefined || value === null || value === '') {
			continue;
		}
		params.set(key, String(value));
	}
	const encoded = params.toString();
	return encoded ? `?${encoded}` : '';
}

async function getJSON<T>(input: string): Promise<T> {
	const response = await fetch(input, {
		credentials: 'same-origin',
		headers: {
			accept: 'application/json'
		}
	});
	if (!response.ok) {
		throw new Error(`Request failed: ${response.status}`);
	}
	return (await response.json()) as T;
}

async function mutateJSON<T>(input: string, init?: RequestInit): Promise<T> {
	const response = await fetch(input, {
		method: 'POST',
		credentials: 'same-origin',
		headers: {
			accept: 'application/json',
			'X-Requested-With': 'pgkit-admin-ui',
			...(init?.headers ?? {})
		},
		...init
	});
	if (!response.ok) {
		let message = `Request failed: ${response.status}`;
		try {
			const data = (await response.json()) as { error?: string };
			if (data.error) {
				message = data.error;
			}
		} catch {
			// ignore response decode failure
		}
		throw new Error(message);
	}
	return (await response.json()) as T;
}

export function getSnapshot(): Promise<DashboardSnapshot> {
	return getJSON<DashboardSnapshot>('/api/dashboard/snapshot');
}

export function getQueueSummary(): Promise<QueueSummary> {
	return getJSON<QueueSummary>('/api/dashboard/queue/summary');
}

export function getQueueJobs(filters: {
	queue?: string;
	status?: string;
	search?: string;
	limit?: number;
	offset?: number;
}): Promise<QueueJobsResponse> {
	return getJSON<QueueJobsResponse>(`/api/dashboard/queue/jobs${queryString(filters)}`);
}

export function getQueueLocks(): Promise<QueueLock[]> {
	return getJSON<QueueLock[]>('/api/dashboard/queue/locks');
}

export function getWorkflowRuns(filters: {
	workflow_name?: string;
	status?: string;
	search?: string;
	limit?: number;
	offset?: number;
}): Promise<WorkflowRunsResponse> {
	return getJSON<WorkflowRunsResponse>(`/api/dashboard/workflow/runs${queryString(filters)}`);
}

export function getWorkflowRun(runID: string): Promise<WorkflowRunGraphView> {
	return getJSON<WorkflowRunGraphView>(`/api/dashboard/workflow/runs/${encodeURIComponent(runID)}`);
}

export function replayQueueJob(jobID: number): Promise<void> {
	return mutateJSON<unknown>(`/api/dashboard/queue/jobs/${jobID}/replay`).then(() => undefined);
}

export function retryWorkflowRun(runID: string) {
	return mutateJSON(`/api/dashboard/workflow/runs/${encodeURIComponent(runID)}/retry`);
}

export function retryWorkflowStep(stepID: number) {
	return mutateJSON(`/api/dashboard/workflow/steps/${stepID}/retry`);
}
