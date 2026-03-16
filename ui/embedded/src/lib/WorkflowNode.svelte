<script lang="ts">
	import { Handle, Position } from '@xyflow/svelte';
	import {
		AlertCircleIcon,
		CheckCircle2Icon,
		ClockIcon,
		GitBranchPlusIcon,
		LayersIcon,
		RotateCwIcon,
		SkipForwardIcon
	} from 'lucide-svelte';

	let { data, isConnectable } = $props<any>();

	let statusColor = $derived.by(() => {
		switch (data.status) {
			case 'succeeded': return 'text-success-600 bg-success-50';
			case 'failed': return 'text-error-600 bg-error-50';
			case 'running': return 'text-primary-600 bg-primary-50';
			case 'waiting_retry': return 'text-warning-600 bg-warning-50';
			case 'queued': return 'text-secondary-700 bg-secondary-50';
			default: return 'text-surface-600 bg-surface-100';
		}
	});

</script>

<Handle type="target" position={Position.Top} {isConnectable} class="h-3 w-3 border-2 border-white bg-surface-300 shadow-sm" />

<div class="w-[348px] rounded-[1.35rem] border border-white/80 bg-white/92 p-4 text-left shadow-[0_18px_48px_-24px_rgba(15,23,42,0.45)] backdrop-blur-xl transition-all duration-200 hover:-translate-y-0.5 hover:border-primary-200 hover:shadow-[0_24px_60px_-24px_rgba(15,23,42,0.5)]">
	<div class="mb-3 flex items-start justify-between gap-4">
		<div class="min-w-0">
			<div class="mb-1 flex items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.2em] text-surface-500">
				<span>{data.kind}</span>
				{#if data.queue}
					<span class="inline-flex items-center gap-1 rounded-full bg-surface-100 px-2 py-0.5 text-[9px] font-bold tracking-[0.18em] text-surface-600">
						<LayersIcon size={11} /> {data.queue}
					</span>
				{/if}
			</div>
			<div class="truncate text-[1.02rem] font-semibold text-surface-900" title={data.label}>
				{data.label}
			</div>
			<div class="mt-1 font-mono text-[11px] text-surface-400">{data.id}</div>
		</div>
		<div class="flex shrink-0 items-center gap-1.5 rounded-full px-2.5 py-1 text-[10px] font-bold uppercase {statusColor}">
			{#if data.status === 'succeeded'}
				<CheckCircle2Icon size={14} />
			{:else if data.status === 'failed'}
				<AlertCircleIcon size={14} />
			{:else if data.status === 'running'}
				<RotateCwIcon size={14} class="animate-spin" />
			{:else if data.status === 'waiting_retry'}
				<ClockIcon size={14} />
			{:else if data.status === 'queued'}
				<GitBranchPlusIcon size={14} />
			{:else}
				<SkipForwardIcon size={14} />
			{/if}
			{data.status}
		</div>
	</div>

	<div class="grid gap-2 sm:grid-cols-2">
		<div class="rounded-2xl border border-surface-200/80 bg-surface-50/85 px-3 py-2.5">
			<div class="text-[10px] font-bold uppercase tracking-[0.18em] text-surface-400">Attempts</div>
			<div class="mt-1 text-sm font-semibold text-surface-800">{data.attemptText}</div>
		</div>
		<div class="rounded-2xl border border-surface-200/80 bg-surface-50/85 px-3 py-2.5">
			<div class="text-[10px] font-bold uppercase tracking-[0.18em] text-surface-400">Depends On</div>
			<div class="mt-1 truncate text-sm font-semibold text-surface-800" title={data.dependsOn?.join(', ')}>
				{data.dependsOn?.length ? data.dependsOn.join(', ') : 'Root step'}
			</div>
		</div>
	</div>

	{#if data.fanout.total > 0}
		<div class="mt-3 rounded-2xl border border-secondary-100 bg-secondary-50/70 p-3 text-[11px]">
			<div class="mb-2 flex items-center justify-between gap-3">
				<span class="font-semibold uppercase tracking-[0.18em] text-secondary-700">Fan-Out Items</span>
				<span class="font-bold text-secondary-900">{data.fanout.succeeded}/{data.fanout.total}</span>
			</div>
			<div class="grid grid-cols-4 gap-2 text-center text-[10px]">
				<div class="rounded-xl bg-white/75 px-2 py-1.5">
					<div class="font-bold text-surface-800">{data.fanout.running}</div>
					<div class="text-surface-400">Run</div>
				</div>
				<div class="rounded-xl bg-white/75 px-2 py-1.5">
					<div class="font-bold text-warning-700">{data.fanout.waiting_retry}</div>
					<div class="text-surface-400">Retry</div>
				</div>
				<div class="rounded-xl bg-white/75 px-2 py-1.5">
					<div class="font-bold text-error-700">{data.fanout.failed}</div>
					<div class="text-surface-400">Fail</div>
				</div>
				<div class="rounded-xl bg-white/75 px-2 py-1.5">
					<div class="font-bold text-success-700">{data.fanout.succeeded}</div>
					<div class="text-surface-400">Done</div>
				</div>
			</div>
		</div>
	{/if}
</div>

<Handle type="source" position={Position.Bottom} {isConnectable} class="h-3 w-3 border-2 border-white bg-surface-300 shadow-sm" />
