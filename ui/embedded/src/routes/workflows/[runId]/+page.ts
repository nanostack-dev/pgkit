import type { PageLoad } from './$types';
import type { WorkflowRunDetail } from '$lib/types';

export const load: PageLoad = async ({ fetch, params }) => {
	const response = await fetch(`/api/dashboard/workflow/runs/${encodeURIComponent(params.runId)}`);
	if (!response.ok) {
		throw new Error(`Failed to load workflow run: ${response.status}`);
	}
	return {
		detail: (await response.json()) as WorkflowRunDetail
	};
};
