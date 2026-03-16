import type { PageLoad } from './$types';
import type { DashboardSnapshot } from '$lib/types';

export const load: PageLoad = async ({ fetch }) => {
	const response = await fetch('/api/dashboard/snapshot');
	if (!response.ok) {
		throw new Error(`Failed to load dashboard snapshot: ${response.status}`);
	}
	return {
		snapshot: (await response.json()) as DashboardSnapshot
	};
};
