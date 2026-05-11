<script lang="ts">
	import { onMount } from 'svelte';
	import { getWorkflowRuns, retryWorkflowRun } from '$lib/api';
	import { formatDateTime } from '$lib/format';
	import { workflowRunTone } from '$lib/status';
	import type { WorkflowRun } from '$lib/types';
	import { 
		WorkflowIcon, 
		SearchIcon, 
		FilterIcon, 
		ChevronLeftIcon, 
		ChevronRightIcon,
		RefreshCwIcon,
		CheckCircle2Icon,
		AlertTriangleIcon
	} from 'lucide-svelte';

	let workflowName = $state('');
	let status = $state('');
	let search = $state('');
	let runs = $state<WorkflowRun[]>([]);
	let total = $state(0);
	let error = $state('');
	let loading = $state(true);
	let retryingRunID = $state<string | null>(null);
	const limit = 20;
	let offset = $state(0);

	async function refresh() {
		loading = true;
		error = '';
		try {
			const response = await getWorkflowRuns({ workflow_name: workflowName, status, search, limit, offset });
			runs = response.items;
			total = response.total;
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to load workflow runs.';
		} finally {
			loading = false;
		}
	}

	function applyFilters(event: SubmitEvent) {
		event.preventDefault();
		offset = 0;
		void refresh();
	}

	function move(delta: number) {
		offset = Math.max(0, offset + delta);
		void refresh();
	}

	async function retryRun(runID: string) {
		retryingRunID = runID;
		error = '';
		try {
			await retryWorkflowRun(runID);
			await refresh();
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to retry workflow run.';
		} finally {
			retryingRunID = null;
		}
	}

	onMount(() => {
		void refresh();
		const timer = window.setInterval(() => void refresh(), 5000);
		return () => window.clearInterval(timer);
	});
</script>

<div class="mb-8">
	<h1 class="text-3xl font-semibold tracking-tight text-surface-900 flex items-center gap-3">
		<div class="bg-secondary-50 text-secondary-600 p-2 rounded-xl border border-secondary-100">
			<WorkflowIcon class="size-6" />
		</div>
		Workflows
	</h1>
	<p class="mt-3 text-surface-500 max-w-2xl text-sm">Monitor version-pinned runs, DAG definitions, and live execution state.</p>
</div>

<div class="bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-3xl shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)] overflow-hidden flex flex-col">
	<div class="p-6 border-b border-surface-200/60 bg-surface-50/50">
		<div class="flex flex-col lg:flex-row lg:items-center justify-between gap-4">
			<div>
				<h3 class="text-lg font-semibold text-surface-900">Run Explorer</h3>
				<p class="text-sm text-surface-500">Query and filter workflow executions.</p>
			</div>
			<form class="flex flex-wrap items-center gap-3" onsubmit={applyFilters}>
				<div class="relative min-w-[200px]">
					<SearchIcon class="size-4 absolute left-3 top-1/2 -translate-y-1/2 text-surface-400" />
					<input class="w-full bg-white border border-surface-200 rounded-xl pl-9 pr-4 py-2 text-sm focus:ring-2 focus:ring-secondary-500/20 focus:border-secondary-500 transition-all outline-none" bind:value={search} placeholder="Search ID or correlation..." />
				</div>
				<div class="relative min-w-[160px]">
					<FilterIcon class="size-4 absolute left-3 top-1/2 -translate-y-1/2 text-surface-400" />
					<input class="w-full bg-white border border-surface-200 rounded-xl pl-9 pr-4 py-2 text-sm focus:ring-2 focus:ring-secondary-500/20 focus:border-secondary-500 transition-all outline-none" bind:value={workflowName} placeholder="Workflow name" />
				</div>
				<select class="bg-white border border-surface-200 rounded-xl px-4 py-2 text-sm focus:ring-2 focus:ring-secondary-500/20 focus:border-secondary-500 transition-all outline-none min-w-[140px] appearance-none" bind:value={status}>
					<option value="">All statuses</option>
					<option value="running">Running</option>
					<option value="succeeded">Succeeded</option>
					<option value="failed">Failed</option>
					<option value="cancelled">Cancelled</option>
				</select>
				<button class="bg-secondary-600 hover:bg-secondary-700 text-white px-5 py-2 rounded-xl text-sm font-medium transition-colors shadow-sm shadow-secondary-500/20" type="submit">Filter</button>
			</form>
		</div>
	</div>

	{#if error}
		<div class="m-6 p-4 rounded-xl bg-error-50 border border-error-200 text-error-800 text-sm flex items-center gap-3">
			<AlertTriangleIcon class="size-5 text-error-600" />
			{error}
		</div>
	{/if}

	<div class="overflow-x-auto">
		<table class="w-full text-left border-collapse min-w-[1000px]">
			<thead>
				<tr class="bg-surface-50/30 border-b border-surface-200/60 text-[0.7rem] uppercase tracking-wider text-surface-500">
					<th class="px-6 py-4 font-semibold">Run ID</th>
					<th class="px-6 py-4 font-semibold">Workflow</th>
					<th class="px-6 py-4 font-semibold">Status</th>
					<th class="px-6 py-4 font-semibold">Started</th>
					<th class="px-6 py-4 font-semibold">Completed</th>
					<th class="px-6 py-4 font-semibold">Created By</th>
					<th class="px-6 py-4 font-semibold">Correlation Key</th>
					<th class="px-6 py-4 font-semibold text-right">Actions</th>
				</tr>
			</thead>
			<tbody class="divide-y divide-surface-200/40 text-sm">
				{#if loading && runs.length === 0}
					<tr><td colspan="8" class="px-6 py-12 text-center text-surface-400">
						<RefreshCwIcon class="size-6 animate-spin mx-auto mb-2 opacity-50" />
						Loading workflow runs...
					</td></tr>
				{:else if runs.length === 0}
					<tr><td colspan="8" class="px-6 py-16 text-center text-surface-500">
						<div class="flex flex-col items-center justify-center gap-3">
							<div class="bg-surface-100 p-3 rounded-full"><SearchIcon class="size-6 text-surface-400" /></div>
							<p>No workflow runs match this filter set.</p>
						</div>
					</td></tr>
				{:else}
					{#each runs as run}
						<tr class="hover:bg-surface-50/50 transition-colors group">
							<td class="px-6 py-4">
								<a href={`/workflows/${run.id}`} class="font-mono text-xs text-secondary-600 hover:text-secondary-800 bg-secondary-50 group-hover:bg-secondary-100 px-2 py-1 rounded-md border border-secondary-100 group-hover:border-secondary-200 transition-all font-medium flex items-center gap-1.5 w-fit">
									{run.id}
								</a>
							</td>
							<td class="px-6 py-4">
								<div class="font-medium text-surface-900">{run.workflow_name}</div>
								<div class="text-[0.65rem] text-surface-500 font-bold bg-surface-100 px-1.5 py-0.5 rounded w-fit mt-1">v{run.workflow_version}</div>
							</td>
							<td class="px-6 py-4">
								<span class={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[0.65rem] font-bold uppercase tracking-wider ${
									run.status === 'failed' ? 'bg-error-50 text-error-700 ring-1 ring-error-500/20' : 
									run.status === 'succeeded' ? 'bg-success-50 text-success-700 ring-1 ring-success-500/20' : 
									run.status === 'running' ? 'bg-primary-50 text-primary-700 ring-1 ring-primary-500/20' : 
									'bg-surface-100 text-surface-700 ring-1 ring-surface-500/20'
								}`}>
									{run.status}
								</span>
							</td>
							<td class="px-6 py-4 text-surface-700">
								{formatDateTime(run.started_at)}
							</td>
							<td class="px-6 py-4 text-surface-500">
								{formatDateTime(run.completed_at) || '-'}
							</td>
							<td class="px-6 py-4 text-surface-500">
								{run.created_by ?? '-'}
							</td>
							<td class="px-6 py-4">
								<span class="font-mono text-xs text-surface-500">{run.correlation_key ?? '-'}</span>
							</td>
							<td class="px-6 py-4 text-right">
								{#if run.status === 'failed' || run.status === 'cancelled'}
									<button
										class="inline-flex items-center gap-1.5 rounded-lg border border-surface-200 bg-white px-3 py-1.5 text-xs font-medium text-surface-700 transition-colors hover:bg-surface-50 disabled:opacity-50"
										onclick={() => retryRun(run.id)}
										disabled={retryingRunID === run.id}
									>
										<RefreshCwIcon class={`size-3.5 ${retryingRunID === run.id ? 'animate-spin' : ''}`} />
										Retry Run
									</button>
								{:else}
									<span class="text-surface-300">-</span>
								{/if}
							</td>
						</tr>
					{/each}
				{/if}
			</tbody>
		</table>
	</div>

	<div class="p-4 border-t border-surface-200/60 bg-surface-50/50 flex items-center justify-between">
		<p class="text-sm text-surface-600 font-medium ml-2">
			Showing <span class="text-surface-900">{Math.min(offset + 1, total)}-{Math.min(offset + limit, total)}</span> of <span class="text-surface-900">{total}</span> runs
		</p>
		<div class="flex gap-2">
			<button class="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium rounded-lg text-surface-600 hover:bg-surface-200 hover:text-surface-900 disabled:opacity-50 disabled:cursor-not-allowed transition-colors" disabled={offset === 0} onclick={() => move(-limit)}>
				<ChevronLeftIcon class="size-4" /> Prev
			</button>
			<button class="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium rounded-lg text-surface-600 hover:bg-surface-200 hover:text-surface-900 disabled:opacity-50 disabled:cursor-not-allowed transition-colors" disabled={offset + limit >= total} onclick={() => move(limit)}>
				Next <ChevronRightIcon class="size-4" />
			</button>
		</div>
	</div>
</div>
