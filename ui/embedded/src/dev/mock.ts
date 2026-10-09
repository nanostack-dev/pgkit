import { ApiError } from '../api/client';
import { asJob, demoDataset, emptyDataset, worstDataset, type Dataset } from './fixtures';
import { getDataMode, type DataMode } from './mode';

const cache = new Map<DataMode, Dataset>();

function dataset(mode: DataMode): Dataset {
	let data = cache.get(mode);
	if (!data) {
		data = mode === 'worst' ? worstDataset() : mode === 'empty' ? emptyDataset() : demoDataset();
		cache.set(mode, data);
	}
	return data;
}

function page<T>(items: T[], params: URLSearchParams, pageSize: number) {
	const limit = Number(params.get('limit') ?? pageSize);
	const offset = Number(params.get('offset') ?? 0);
	return { items: items.slice(offset, offset + limit), total: items.length, limit, offset };
}

function matches(text: string | null | undefined, search: string) {
	return (text ?? '').toLowerCase().includes(search.toLowerCase());
}

export async function mockResponse(method: string, path: string, _body: unknown): Promise<unknown> {
	const mode = getDataMode();
	if (mode === 'live') return undefined;
	await new Promise((resolve) => setTimeout(resolve, 120 + Math.random() * 180));
	const data = dataset(mode);
	const url = new URL(path, window.location.origin);
	const route = url.pathname.replace('/api/dashboard/', '');
	const params = url.searchParams;

	if (method !== 'GET') {
		if (!data.config.mutations_enabled) throw new ApiError(405, 'This action is disabled on this server.');
		if (route === 'queue/jobs') return { id: 9_999 };
		if (method === 'DELETE') return undefined;
		const runId = route.match(/^workflow\/runs\/([^/]+)\//)?.[1];
		if (runId) return data.details.get(decodeURIComponent(runId))?.run;
		return data.jobs[0];
	}

	if (route === 'config') return data.config;
	if (route === 'overview') return data.overview(params.get('range') ?? '1h');
	if (route === 'queue/queues') return { now: new Date().toISOString(), items: data.queues };
	if (route === 'locks') return { now: new Date().toISOString(), items: data.locks };
	if (route === 'workflow/workflows') return { items: data.workflows };

	if (route === 'queue/jobs') {
		const queue = params.get('queue');
		const status = params.get('status');
		const search = params.get('search') ?? '';
		const items = data.jobs
			.filter((job) => (!queue || job.queue_name === queue) && (!status || job.status === status))
			.filter((job) => !search || matches(job.queue_name, search) || matches(job.payload_preview, search) || matches(job.last_error, search))
			.map(asJob);
		return page(items, params, data.config.page_size);
	}

	const jobId = route.match(/^queue\/jobs\/(\d+)$/)?.[1];
	if (jobId) {
		const job = data.jobs.find((item) => item.id === Number(jobId));
		if (!job) throw new ApiError(404, 'job not found');
		return job;
	}

	if (route === 'workflow/runs') {
		const workflow = params.get('workflow');
		const status = params.get('status');
		const search = params.get('search') ?? '';
		const topLevel = params.get('top_level') === 'true';
		const parent = params.get('parent_run_id');
		const items = data.runs
			.filter((run) => (!workflow || run.workflow === workflow) && (!status || run.status === status))
			.filter((run) => (!topLevel || !run.parent_run_id) && (!parent || run.parent_run_id === parent))
			.filter((run) => !search || matches(run.id, search) || matches(run.key, search) || matches(run.workflow, search));
		return page(items, params, data.config.page_size);
	}

	const tree = route.match(/^workflow\/runs\/([^/]+)\/tree$/)?.[1];
	if (tree) {
		const id = decodeURIComponent(tree);
		const known = data.trees.get(id);
		if (known) return { root_id: id, ...known };
		const run = data.runs.find((item) => item.id === id);
		if (!run) throw new ApiError(404, 'run not found');
		return { root_id: id, nodes: [{ ...run, depth: 0 }], truncated: false };
	}

	const runId = route.match(/^workflow\/runs\/([^/]+)$/)?.[1];
	if (runId) {
		const detail = data.details.get(decodeURIComponent(runId));
		if (!detail) throw new ApiError(404, 'run not found');
		return { ...detail, now: new Date().toISOString() };
	}

	throw new ApiError(404, `No fixture for ${route}`);
}
