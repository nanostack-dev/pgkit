<script lang="ts">
	import { onMount } from 'svelte';
	import { getQueueJobs, getQueueLocks, getQueueSummary } from '$lib/api';
	import { formatDateTime, formatRelative } from '$lib/format';
	import { queueStatusTone } from '$lib/status';
	import type { QueueJob, QueueLock, QueueSummary } from '$lib/types';
	import {
		Layers3Icon,
		SearchIcon,
		LockIcon,
		FilterIcon,
		ChevronLeftIcon,
		ChevronRightIcon,
		ClockIcon,
		RefreshCwIcon,
		XCircleIcon,
		CheckCircle2Icon,
		AlertTriangleIcon
	} from 'lucide-svelte';

	let queue = $state('');
	let status = $state('');
	let search = $state('');
	let loading = $state(true);
	let error = $state('');
	let summary = $state<QueueSummary | null>(null);
	let locks = $state<QueueLock[]>([]);
	let jobs = $state<QueueJob[]>([]);
	let total = $state(0);
	const limit = 20;
	let offset = $state(0);

	async function refresh() {
		loading = true;
		error = '';
		try {
			const [nextSummary, nextJobs, nextLocks] = await Promise.all([
				getQueueSummary(),
				getQueueJobs({ queue, status, search, limit, offset }),
				getQueueLocks()
			]);
			summary = nextSummary;
			jobs = nextJobs.items;
			total = nextJobs.total;
			locks = nextLocks;
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to load queue data.';
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

	onMount(() => {
		void refresh();
		const timer = window.setInterval(() => void refresh(), 4000);
		return () => window.clearInterval(timer);
	});
</script>

<div class="mb-8">
	<h1 class="text-3xl font-semibold tracking-tight text-surface-900 flex items-center gap-3">
		<div class="bg-primary-50 text-primary-600 p-2 rounded-xl border border-primary-100">
			<Layers3Icon class="size-6" />
		</div>
		Queues
	</h1>
	<p class="mt-3 text-surface-500 max-w-2xl text-sm">Inspect durable jobs, retry pressure, and advisory lock visibility.</p>
</div>

<div class="grid xl:grid-cols-3 gap-6 mb-8">
	<div class="xl:col-span-2 bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-3xl p-6 shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)] flex flex-col">
		<h2 class="text-sm font-semibold uppercase tracking-wider text-surface-500 mb-6">Queue Health</h2>
		{#if summary}
			<div class="grid grid-cols-2 lg:grid-cols-4 gap-4 mt-auto">
				<div class="bg-surface-50 p-4 rounded-2xl border border-surface-100">
					<div class="flex items-center gap-2 mb-2 text-surface-600"><ClockIcon class="size-4" /><span class="text-xs font-semibold uppercase">Pending</span></div>
					<div class="text-3xl font-bold text-surface-900">{summary.pending_jobs}</div>
				</div>
				<div class="bg-warning-50 p-4 rounded-2xl border border-warning-100">
					<div class="flex items-center gap-2 mb-2 text-warning-700"><RefreshCwIcon class="size-4" /><span class="text-xs font-semibold uppercase">Processing</span></div>
					<div class="text-3xl font-bold text-warning-900">{summary.processing_jobs}</div>
				</div>
				<div class="bg-error-50 p-4 rounded-2xl border border-error-100 relative overflow-hidden">
					<div class="flex items-center gap-2 mb-2 text-error-700"><XCircleIcon class="size-4" /><span class="text-xs font-semibold uppercase">Failed</span></div>
					<div class="text-3xl font-bold text-error-900">{summary.failed_jobs}</div>
				</div>
				<div class="bg-success-50 p-4 rounded-2xl border border-success-100">
					<div class="flex items-center gap-2 mb-2 text-success-700"><CheckCircle2Icon class="size-4" /><span class="text-xs font-semibold uppercase">Done</span></div>
					<div class="text-3xl font-bold text-success-900">{summary.done_jobs}</div>
				</div>
			</div>
		{/if}
	</div>

	<div class="bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-3xl p-6 shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)] flex flex-col max-h-[240px]">
		<div class="flex items-center justify-between mb-4">
			<h2 class="text-sm font-semibold uppercase tracking-wider text-surface-500 flex items-center gap-2"><LockIcon class="size-4" /> Advisory Locks</h2>
			<span class="bg-surface-100 text-surface-600 text-xs font-bold px-2 py-0.5 rounded-full">{locks.length}</span>
		</div>
		
		<div class="flex-1 overflow-y-auto pr-2 space-y-2 relative">
			{#if locks.length === 0}
				<div class="absolute inset-0 flex flex-col items-center justify-center text-surface-400">
					<LockIcon class="size-8 opacity-20 mb-2" />
					<p class="text-sm">No active locks.</p>
				</div>
			{:else}
				{#each locks as lock}
					<div class="flex items-center justify-between p-3 rounded-xl bg-surface-50 border border-surface-100/50 hover:border-surface-200 transition-colors">
						<div class="flex items-center gap-3">
							<div class="bg-white p-1.5 rounded-lg shadow-sm">
								<LockIcon class="size-3.5 text-surface-400" />
							</div>
							<div>
								<p class="font-mono text-xs font-medium text-surface-900">PID {lock.pid}</p>
								<p class="text-[0.65rem] text-surface-500 uppercase tracking-wider">{lock.mode}</p>
							</div>
						</div>
						<div class="text-right text-[0.65rem] text-surface-500 font-mono">
							<div>cls:{lock.classid}</div>
							<div>obj:{lock.objid}/{lock.objsubid}</div>
						</div>
					</div>
				{/each}
			{/if}
		</div>
	</div>
</div>

<div class="bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-3xl shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)] overflow-hidden flex flex-col">
	<div class="p-6 border-b border-surface-200/60 bg-surface-50/50">
		<div class="flex flex-col lg:flex-row lg:items-center justify-between gap-4">
			<div>
				<h3 class="text-lg font-semibold text-surface-900">Job Explorer</h3>
				<p class="text-sm text-surface-500">Query and filter the active queue snapshot.</p>
			</div>
			<form class="flex flex-wrap items-center gap-3" onsubmit={applyFilters}>
				<div class="relative min-w-[200px]">
					<SearchIcon class="size-4 absolute left-3 top-1/2 -translate-y-1/2 text-surface-400" />
					<input class="w-full bg-white border border-surface-200 rounded-xl pl-9 pr-4 py-2 text-sm focus:ring-2 focus:ring-primary-500/20 focus:border-primary-500 transition-all outline-none" bind:value={search} placeholder="Search payload or error..." />
				</div>
				<div class="relative min-w-[160px]">
					<FilterIcon class="size-4 absolute left-3 top-1/2 -translate-y-1/2 text-surface-400" />
					<input class="w-full bg-white border border-surface-200 rounded-xl pl-9 pr-4 py-2 text-sm focus:ring-2 focus:ring-primary-500/20 focus:border-primary-500 transition-all outline-none" bind:value={queue} placeholder="Queue name" />
				</div>
				<select class="bg-white border border-surface-200 rounded-xl px-4 py-2 text-sm focus:ring-2 focus:ring-primary-500/20 focus:border-primary-500 transition-all outline-none min-w-[140px] appearance-none" bind:value={status}>
					<option value="">All statuses</option>
					<option value="pending">Pending</option>
					<option value="processing">Processing</option>
					<option value="done">Done</option>
					<option value="failed">Failed</option>
				</select>
				<button class="bg-primary-600 hover:bg-primary-700 text-white px-5 py-2 rounded-xl text-sm font-medium transition-colors shadow-sm shadow-primary-500/20" type="submit">Filter</button>
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
					<th class="px-6 py-4 font-semibold">ID</th>
					<th class="px-6 py-4 font-semibold">Queue</th>
					<th class="px-6 py-4 font-semibold">Status</th>
					<th class="px-6 py-4 font-semibold">Attempts</th>
					<th class="px-6 py-4 font-semibold">Available</th>
					<th class="px-6 py-4 font-semibold">Claimed By</th>
					<th class="px-6 py-4 font-semibold max-w-[200px]">Payload</th>
					<th class="px-6 py-4 font-semibold max-w-[200px]">Last Error</th>
				</tr>
			</thead>
			<tbody class="divide-y divide-surface-200/40 text-sm">
				{#if loading && jobs.length === 0}
					<tr><td colspan="8" class="px-6 py-12 text-center text-surface-400">
						<RefreshCwIcon class="size-6 animate-spin mx-auto mb-2 opacity-50" />
						Loading queue snapshot...
					</td></tr>
				{:else if jobs.length === 0}
					<tr><td colspan="8" class="px-6 py-16 text-center text-surface-500">
						<div class="flex flex-col items-center justify-center gap-3">
							<div class="bg-surface-100 p-3 rounded-full"><SearchIcon class="size-6 text-surface-400" /></div>
							<p>No jobs match this filter set.</p>
						</div>
					</td></tr>
				{:else}
					{#each jobs as job}
						<tr class="hover:bg-surface-50/50 transition-colors group">
							<td class="px-6 py-4">
								<span class="font-mono text-xs text-surface-500 bg-surface-100 group-hover:bg-white px-2 py-1 rounded-md border border-transparent group-hover:border-surface-200 transition-all">{job.id}</span>
							</td>
							<td class="px-6 py-4 font-medium text-surface-700">{job.queue_name}</td>
							<td class="px-6 py-4">
								<span class={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[0.65rem] font-bold uppercase tracking-wider ${
									job.status === 'failed' ? 'bg-error-50 text-error-700 ring-1 ring-error-500/20' : 
									job.status === 'done' ? 'bg-success-50 text-success-700 ring-1 ring-success-500/20' : 
									job.status === 'processing' ? 'bg-warning-50 text-warning-700 ring-1 ring-warning-500/20' : 
									'bg-primary-50 text-primary-700 ring-1 ring-primary-500/20'
								}`}>
									{job.status}
								</span>
							</td>
							<td class="px-6 py-4 text-surface-600">
								<span class="{job.attempts >= job.max_attempts ? 'text-error-600 font-medium' : ''}">{job.attempts}</span><span class="text-surface-400">/{job.max_attempts}</span>
							</td>
							<td class="px-6 py-4">
								<div class="text-surface-800">{formatDateTime(job.available_at)}</div>
								<div class="text-[0.65rem] text-surface-500 mt-0.5">{formatRelative(job.available_at)}</div>
							</td>
							<td class="px-6 py-4 text-surface-600">
								{#if job.claimed_by}
									<span class="inline-flex items-center gap-1.5 bg-surface-100 px-2 py-1 rounded text-xs font-mono text-surface-600">
										<LockIcon class="size-3" />
										{job.claimed_by.split('-')[0]}...
									</span>
								{:else}
									<span class="text-surface-400">-</span>
								{/if}
							</td>
							<td class="px-6 py-4">
								<div class="font-mono max-w-[200px] truncate text-[0.65rem] bg-surface-100/50 p-1.5 rounded border border-surface-200/50 text-surface-600" title={job.payload_preview}>{job.payload_preview}</div>
							</td>
							<td class="px-6 py-4">
								{#if job.last_error}
									<div class="max-w-[200px] truncate text-[0.7rem] text-error-600 font-medium" title={job.last_error}>{job.last_error}</div>
								{:else}
									<span class="text-surface-400">-</span>
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
			Showing <span class="text-surface-900">{Math.min(offset + 1, total)}-{Math.min(offset + limit, total)}</span> of <span class="text-surface-900">{total}</span> jobs
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