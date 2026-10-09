import { keepPreviousData, useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { del, get, post, withQuery } from './client';
import type {
	AdminConfig,
	EnqueueRequest,
	JobFilters,
	LocksResponse,
	Overview,
	OverviewRange,
	Page,
	QueueJob,
	QueueJobDetail,
	QueuesResponse,
	RunDetail,
	RunFilters,
	RunSummary,
	RunTree,
	WorkflowRun,
	WorkflowStats,
} from './types';

const api = '/api/dashboard';

export const keys = {
	config: ['config'] as const,
	overview: (range: OverviewRange) => ['overview', range] as const,
	queues: ['queues'] as const,
	jobs: (filters: JobFilters) => ['jobs', filters] as const,
	job: (id: number) => ['job', id] as const,
	workflows: ['workflows'] as const,
	runs: (filters: RunFilters) => ['runs', filters] as const,
	run: (id: string) => ['run', id] as const,
	tree: (id: string) => ['tree', id] as const,
	locks: ['locks'] as const,
};

export const liveInterval = {
	overview: 5_000,
	lists: 5_000,
	activeRun: 1_500,
	locks: 3_000,
};

export function useConfig() {
	return useQuery({
		queryKey: keys.config,
		queryFn: ({ signal }) => get<AdminConfig>(`${api}/config`, signal),
		staleTime: Infinity,
	});
}

export function useOverview(range: OverviewRange) {
	return useQuery({
		queryKey: keys.overview(range),
		queryFn: ({ signal }) => get<Overview>(withQuery(`${api}/overview`, { range }), signal),
		refetchInterval: liveInterval.overview,
		placeholderData: keepPreviousData,
	});
}

export function useQueues() {
	return useQuery({
		queryKey: keys.queues,
		queryFn: ({ signal }) => get<QueuesResponse>(`${api}/queue/queues`, signal),
		refetchInterval: liveInterval.lists,
	});
}

export function useJobs(filters: JobFilters) {
	return useQuery({
		queryKey: keys.jobs(filters),
		queryFn: ({ signal }) => get<Page<QueueJob>>(withQuery(`${api}/queue/jobs`, filters), signal),
		refetchInterval: liveInterval.lists,
		placeholderData: keepPreviousData,
	});
}

export function useJob(id: number | null) {
	return useQuery({
		queryKey: keys.job(id ?? 0),
		queryFn: ({ signal }) => get<QueueJobDetail>(`${api}/queue/jobs/${id}`, signal),
		enabled: id !== null,
		refetchInterval: (query) => {
			const status = query.state.data?.status;
			return status === 'pending' || status === 'processing' ? liveInterval.activeRun : false;
		},
	});
}

export function useWorkflowStats(enabled: boolean) {
	return useQuery({
		queryKey: keys.workflows,
		queryFn: ({ signal }) => get<{ items: WorkflowStats[] }>(`${api}/workflow/workflows`, signal),
		refetchInterval: liveInterval.lists,
		enabled,
	});
}

export function useRuns(filters: RunFilters, enabled = true) {
	return useQuery({
		queryKey: keys.runs(filters),
		queryFn: ({ signal }) => get<Page<RunSummary>>(withQuery(`${api}/workflow/runs`, filters), signal),
		refetchInterval: liveInterval.lists,
		placeholderData: keepPreviousData,
		enabled,
	});
}

function runFinished(status: string | undefined) {
	return status === 'succeeded' || status === 'failed' || status === 'cancelled';
}

export function useRun(id: string) {
	return useQuery({
		queryKey: keys.run(id),
		queryFn: ({ signal }) => get<RunDetail>(`${api}/workflow/runs/${encodeURIComponent(id)}`, signal),
		refetchInterval: (query) => {
			const detail = query.state.data;
			if (!detail) return false;
			const childrenActive = detail.children.some((child) => !runFinished(child.status));
			return !runFinished(detail.run.status) || childrenActive ? liveInterval.activeRun : false;
		},
		retry: (count, error) => !(error instanceof Error && 'status' in error && error.status === 404) && count < 2,
	});
}

export function useRunTree(id: string, enabled: boolean, live: boolean) {
	return useQuery({
		queryKey: keys.tree(id),
		queryFn: ({ signal }) => get<RunTree>(`${api}/workflow/runs/${encodeURIComponent(id)}/tree`, signal),
		enabled,
		refetchInterval: live ? liveInterval.activeRun * 2 : false,
	});
}

export function useLocks() {
	return useQuery({
		queryKey: keys.locks,
		queryFn: ({ signal }) => get<LocksResponse>(`${api}/locks`, signal),
		refetchInterval: liveInterval.locks,
	});
}

function useInvalidateAll() {
	const client = useQueryClient();
	return () => client.invalidateQueries({ predicate: (query) => query.queryKey[0] !== 'config' });
}

export function useReplayJob() {
	const invalidate = useInvalidateAll();
	return useMutation({
		mutationFn: (id: number) => post<QueueJob>(`${api}/queue/jobs/${id}/replay`),
		onSettled: invalidate,
	});
}

export function useDeleteJob() {
	const invalidate = useInvalidateAll();
	return useMutation({
		mutationFn: (id: number) => del<void>(`${api}/queue/jobs/${id}`),
		onSettled: invalidate,
	});
}

export function useEnqueueJob() {
	const invalidate = useInvalidateAll();
	return useMutation({
		mutationFn: (request: EnqueueRequest) => post<{ id: number }>(`${api}/queue/jobs`, request),
		onSettled: invalidate,
	});
}

export function useRetryRun() {
	const invalidate = useInvalidateAll();
	return useMutation({
		mutationFn: (id: string) => post<WorkflowRun>(`${api}/workflow/runs/${encodeURIComponent(id)}/retry`),
		onSettled: invalidate,
	});
}

export function useCancelRun() {
	const invalidate = useInvalidateAll();
	return useMutation({
		mutationFn: (id: string) => post<WorkflowRun>(`${api}/workflow/runs/${encodeURIComponent(id)}/cancel`),
		onSettled: invalidate,
	});
}
