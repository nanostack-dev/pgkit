<script lang="ts">
	import { Handle, Position } from '@xyflow/svelte';
	import type { NodeProps } from '@xyflow/svelte';
	import { CheckCircle2Icon, ClockIcon, PlayCircleIcon, AlertCircleIcon, RotateCwIcon, XCircleIcon, SkipForwardIcon } from 'lucide-svelte';

	let { data, isConnectable } = $props<any>();

	let statusColor = $derived.by(() => {
		switch (data.status) {
			case 'succeeded': return 'text-success-600 bg-success-50';
			case 'failed': return 'text-error-600 bg-error-50';
			case 'running': return 'text-primary-600 bg-primary-50';
			case 'waiting_retry': return 'text-warning-600 bg-warning-50';
			default: return 'text-surface-600 bg-surface-100';
		}
	});

</script>

<Handle type="target" position={Position.Top} {isConnectable} class="w-3 h-3 bg-surface-300" />

<div class="p-4 bg-white border border-surface-300 rounded-xl shadow-sm w-[320px] text-left hover:border-primary-400 hover:shadow-md transition-all duration-200">
	<div class="flex justify-between items-start mb-2">
		<div class="text-[10px] uppercase tracking-wider text-surface-500 font-semibold">{data.kind}</div>
		<div class="flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[10px] font-bold uppercase {statusColor}">
			{#if data.status === 'succeeded'}
				<CheckCircle2Icon size={14} />
			{:else if data.status === 'failed'}
				<AlertCircleIcon size={14} />
			{:else if data.status === 'running'}
				<RotateCwIcon size={14} class="animate-spin" />
			{:else if data.status === 'waiting_retry'}
				<ClockIcon size={14} />
			{:else}
				<SkipForwardIcon size={14} />
			{/if}
			{data.status}
		</div>
	</div>
	
	<div class="text-base font-semibold text-surface-900 whitespace-nowrap overflow-hidden text-ellipsis max-w-[250px]" title={data.label}>
		{data.label}
	</div>
	<div class="text-[11px] font-mono text-surface-400 mt-1">{data.id}</div>
	
	<div class="mt-3 text-xs text-surface-600 border-t border-surface-200 pt-2 flex justify-between items-center">
		<span>{data.attemptText}</span>
		{#if data.status === 'failed' && data.onRetry}
			<button class="text-primary-600 hover:text-primary-700 font-medium hover:underline flex items-center gap-1" onclick={() => data.onRetry?.()}>
				<RotateCwIcon size={12} /> Retry
			</button>
		{/if}
	</div>
	
	{#if data.fanout.total > 0}
		<div class="mt-3 text-[11px] p-2 bg-surface-50/50 rounded-lg flex justify-between border border-surface-100">
			<span class="text-surface-500">Fan-out:</span>
			<span class="font-bold text-surface-700">{data.fanout.succeeded}/{data.fanout.total}</span>
		</div>
	{/if}
</div>

<Handle type="source" position={Position.Bottom} {isConnectable} class="w-3 h-3 bg-surface-300" />
