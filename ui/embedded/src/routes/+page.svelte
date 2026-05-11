<script lang="ts">
	import type { DashboardSnapshot } from '$lib/types';
	import { formatCount, formatDateTime } from '$lib/format';
	import { queueStatusTone, workflowRunTone } from '$lib/status';
	import { 
		Layers3Icon, 
		LockIcon, 
		WorkflowIcon, 
		ListTreeIcon,
		ClockIcon,
		CheckCircle2Icon,
		RefreshCwIcon,
		XCircleIcon,
		ArrowRightIcon,
		ChevronRightIcon
	} from 'lucide-svelte';

	let { data } = $props<{ data: { snapshot: DashboardSnapshot } }>();
</script>

<div class="mb-8 flex flex-col md:flex-row md:items-center justify-between gap-4">
	<div>
		<h1 class="text-3xl font-semibold tracking-tight text-surface-900">Control Center</h1>
		<p class="mt-2 text-surface-500 max-w-2xl text-sm">Monitor your distributed Postgres-native tasks, workflows, and infrastructure locks in real-time.</p>
	</div>
</div>

<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
	<!-- Pending -->
	<div class="bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-2xl p-5 shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)] hover:shadow-[0_8px_30px_-12px_rgba(0,0,0,0.1)] transition-all">
		<div class="flex items-center justify-between mb-4">
			<div class="bg-surface-100 text-surface-600 p-2 rounded-xl">
				<ClockIcon class="size-5" />
			</div>
			<span class="text-xs font-semibold uppercase tracking-wider text-surface-400">Pending</span>
		</div>
		<div class="text-3xl font-bold text-surface-900">{formatCount(data.snapshot.queue.summary.pending_jobs)}</div>
		<p class="text-sm text-surface-500 mt-1">Jobs awaiting execution</p>
	</div>

	<!-- Processing -->
	<div class="bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-2xl p-5 shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)] hover:shadow-[0_8px_30px_-12px_rgba(0,0,0,0.1)] transition-all">
		<div class="flex items-center justify-between mb-4">
			<div class="bg-warning-50 text-warning-600 p-2 rounded-xl">
				<RefreshCwIcon class="size-5" />
			</div>
			<span class="text-xs font-semibold uppercase tracking-wider text-surface-400">Processing</span>
		</div>
		<div class="text-3xl font-bold text-surface-900">{formatCount(data.snapshot.queue.summary.processing_jobs)}</div>
		<p class="text-sm text-surface-500 mt-1">Active worker tasks</p>
	</div>

	<!-- Failed -->
	<div class="bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-2xl p-5 shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)] hover:shadow-[0_8px_30px_-12px_rgba(0,0,0,0.1)] transition-all relative overflow-hidden group">
		{#if data.snapshot.queue.summary.failed_jobs > 0}
			<div class="absolute inset-0 bg-error-50/50 -z-10 group-hover:bg-error-50 transition-colors"></div>
		{/if}
		<div class="flex items-center justify-between mb-4">
			<div class="bg-error-50 text-error-600 p-2 rounded-xl">
				<XCircleIcon class="size-5" />
			</div>
			<span class="text-xs font-semibold uppercase tracking-wider text-surface-400">Failed</span>
		</div>
		<div class="text-3xl font-bold {data.snapshot.queue.summary.failed_jobs > 0 ? 'text-error-600' : 'text-surface-900'}">{formatCount(data.snapshot.queue.summary.failed_jobs)}</div>
		<p class="text-sm {data.snapshot.queue.summary.failed_jobs > 0 ? 'text-error-500' : 'text-surface-500'} mt-1">Tasks requiring attention</p>
	</div>

	<!-- Locks -->
	<div class="bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-2xl p-5 shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)] hover:shadow-[0_8px_30px_-12px_rgba(0,0,0,0.1)] transition-all">
		<div class="flex items-center justify-between mb-4">
			<div class="bg-primary-50 text-primary-600 p-2 rounded-xl">
				<LockIcon class="size-5" />
			</div>
			<span class="text-xs font-semibold uppercase tracking-wider text-surface-400">Advisory Locks</span>
		</div>
		<div class="text-3xl font-bold text-surface-900">{formatCount(data.snapshot.queue.summary.advisory_locks)}</div>
		<p class="text-sm text-surface-500 mt-1">Active pg_advisory_locks</p>
	</div>
</div>

<div class="grid xl:grid-cols-2 gap-8">
	<!-- Recent Queues -->
	<div class="flex flex-col h-full bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-3xl overflow-hidden shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)]">
		<div class="p-6 border-b border-surface-200/60 flex items-center justify-between bg-surface-50/50">
			<div>
				<div class="flex items-center gap-2 text-surface-900 font-semibold text-lg">
					<Layers3Icon class="size-5 text-primary-500" />
					Recent Queue Jobs
				</div>
				<p class="text-sm text-surface-500 mt-1">Latest activity across all queues</p>
			</div>
			<a href="/queues" class="flex items-center gap-1.5 text-sm font-medium text-primary-600 hover:text-primary-700 bg-primary-50 hover:bg-primary-100 px-3 py-1.5 rounded-lg transition-colors">
				View All <ArrowRightIcon class="size-3.5" />
			</a>
		</div>
		<div class="flex-1 p-0 overflow-x-auto">
			<table class="w-full text-left border-collapse">
				<thead>
					<tr class="bg-surface-50/80 border-b border-surface-200/60 text-[0.7rem] uppercase tracking-wider text-surface-500">
						<th class="px-6 py-3 font-semibold">Job ID</th>
						<th class="px-6 py-3 font-semibold">Queue</th>
						<th class="px-6 py-3 font-semibold">Status</th>
						<th class="px-6 py-3 font-semibold text-right">Available</th>
					</tr>
				</thead>
				<tbody class="divide-y divide-surface-200/50 text-sm">
					{#each data.snapshot.queue.jobs.items.slice(0, 6) as job}
						<tr class="hover:bg-surface-50/50 transition-colors">
							<td class="px-6 py-3.5">
								<span class="font-mono text-xs text-surface-500 bg-surface-100 px-2 py-1 rounded-md">{job.id}</span>
							</td>
							<td class="px-6 py-3.5 font-medium text-surface-700">{job.queue_name}</td>
							<td class="px-6 py-3.5">
								<span class={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[0.65rem] font-bold uppercase tracking-wider ${
									job.status === 'failed' ? 'bg-error-50 text-error-700 ring-1 ring-error-500/20' : 
									job.status === 'done' ? 'bg-success-50 text-success-700 ring-1 ring-success-500/20' : 
									job.status === 'processing' ? 'bg-warning-50 text-warning-700 ring-1 ring-warning-500/20' : 
									'bg-primary-50 text-primary-700 ring-1 ring-primary-500/20'
								}`}>
									{job.status}
								</span>
							</td>
							<td class="px-6 py-3.5 text-right text-surface-500 whitespace-nowrap">{formatDateTime(job.available_at)}</td>
						</tr>
					{/each}
					{#if data.snapshot.queue.jobs.items.length === 0}
						<tr>
							<td colspan="4" class="px-6 py-12 text-center text-surface-500">
								<div class="flex flex-col items-center justify-center gap-2">
									<CheckCircle2Icon class="size-8 text-surface-300" />
									<p>No jobs in queue.</p>
								</div>
							</td>
						</tr>
					{/if}
				</tbody>
			</table>
		</div>
	</div>

	<!-- Recent Workflows -->
	<div class="flex flex-col h-full bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-3xl overflow-hidden shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)]">
		<div class="p-6 border-b border-surface-200/60 flex items-center justify-between bg-surface-50/50">
			<div>
				<div class="flex items-center gap-2 text-surface-900 font-semibold text-lg">
					<WorkflowIcon class="size-5 text-secondary-500" />
					Recent Workflow Runs
				</div>
				<p class="text-sm text-surface-500 mt-1">Latest DAG executions</p>
			</div>
			<a href="/workflows" class="flex items-center gap-1.5 text-sm font-medium text-secondary-600 hover:text-secondary-700 bg-secondary-50 hover:bg-secondary-100 px-3 py-1.5 rounded-lg transition-colors">
				View All <ArrowRightIcon class="size-3.5" />
			</a>
		</div>
		<div class="flex-1 p-3">
			<div class="space-y-1.5">
				{#each data.snapshot.workflow.runs.items.slice(0, 6) as run}
					<a href={`/workflows/${run.id}`} class="flex items-center justify-between p-4 rounded-2xl hover:bg-surface-50 border border-transparent hover:border-surface-200 transition-all group">
						<div class="flex items-start gap-4">
							<div class="bg-surface-100 p-2.5 rounded-xl group-hover:bg-white group-hover:shadow-sm transition-all text-surface-500">
								<ListTreeIcon class="size-5" />
							</div>
							<div>
								<div class="flex items-center gap-2">
									<h4 class="font-medium text-surface-900">{run.workflow_name}</h4>
									<span class="px-1.5 py-0.5 rounded text-[0.65rem] font-bold bg-surface-100 text-surface-600">v{run.workflow_version}</span>
								</div>
								<div class="flex items-center gap-3 mt-1.5">
									<span class="font-mono text-[0.65rem] text-surface-400 bg-surface-100 px-1.5 py-0.5 rounded">{run.id.substring(0, 12)}...</span>
									<span class="text-[0.7rem] text-surface-500 flex items-center gap-1"><ClockIcon class="size-3" /> {formatDateTime(run.started_at)}</span>
								</div>
							</div>
						</div>
						<div class="flex items-center gap-3">
							<span class={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[0.65rem] font-bold uppercase tracking-wider ${
								run.status === 'failed' ? 'bg-error-50 text-error-700 ring-1 ring-error-500/20' : 
								run.status === 'succeeded' ? 'bg-success-50 text-success-700 ring-1 ring-success-500/20' : 
								run.status === 'running' ? 'bg-primary-50 text-primary-700 ring-1 ring-primary-500/20' : 
								'bg-surface-100 text-surface-700 ring-1 ring-surface-500/20'
							}`}>
								{run.status}
							</span>
							<ChevronRightIcon class="size-4 text-surface-300 group-hover:text-surface-600 transition-colors" />
						</div>
					</a>
				{/each}
				
				{#if data.snapshot.workflow.runs.items.length === 0}
					<div class="py-12 text-center text-surface-500 flex flex-col items-center justify-center gap-2">
						<CheckCircle2Icon class="size-8 text-surface-300" />
						<p>No workflow runs recorded yet.</p>
					</div>
				{/if}
			</div>
		</div>
	</div>
</div>