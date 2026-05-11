import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch, params }) => {
	const response = await fetch(`/api/dashboard/workflow/runs/${params.runId}`);
	if (!response.ok) {
		throw new Error(`Failed to load workflow run: ${response.status}`);
	}
	return {
		runGraph: await response.json()
	};
};
