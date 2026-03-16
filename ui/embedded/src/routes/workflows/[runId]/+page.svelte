<script lang="ts">
	import { formatDateTime, prettyJSON } from '$lib/format';
	import { workflowRunTone, workflowStepTone } from '$lib/status';
	import type { WorkflowRunGraphNode, WorkflowRunGraphView } from '$lib/types';
	import { ArrowLeftIcon, PlayCircleIcon, CheckCircle2Icon, XCircleIcon, ClockIcon, RotateCwIcon, ServerIcon, LayersIcon } from 'lucide-svelte';
	import Graph from '$lib/Graph.svelte';

	let { data } = $props<{ data: { runGraph: WorkflowRunGraphView } }>();

	function nodeChildren(node: WorkflowRunGraphNode) {
		return node.items ?? [];
	}

	let viewMode = $state<'graph' | 'list'>('graph');
</script>

<div class="mb-6">
	<a href="/workflows" class="inline-flex items-center gap-2 text-sm font-medium text-surface-500 hover:text-surface-900 transition-colors mb-4">
		<ArrowLeftIcon class="size-4" /> Back to Workflows
	</a>
	<div class="flex flex-col md:flex-row md:items-start justify-between gap-4">
		<div>
			<div class="flex items-center gap-3 mb-1">
				<h1 class="text-3xl font-semibold tracking-tight text-surface-900">{data.runGraph.run.workflow_name}</h1>
				<span class="bg-surface-100 text-surface-600 px-2 py-0.5 rounded-md text-xs font-bold font-mono">v{data.runGraph.run.workflow_version}</span>
			</div>
			<p class="font-mono text-sm text-surface-500 flex items-center gap-2">
				{data.runGraph.run.id}
				{#if data.runGraph.run.correlation_key}
					<span class="text-surface-300">•</span>
					<span class="text-primary-600 bg-primary-50 px-1.5 py-0.5 rounded">{data.runGraph.run.correlation_key}</span>
				{/if}
			</p>
		</div>
		<div class={`inline-flex items-center gap-2 px-4 py-2 rounded-xl text-sm font-bold uppercase tracking-wider ${
			data.runGraph.run.status === 'failed' ? 'bg-error-50 text-error-700 border border-error-200' : 
			data.runGraph.run.status === 'succeeded' ? 'bg-success-50 text-success-700 border border-success-200' : 
			data.runGraph.run.status === 'running' ? 'bg-primary-50 text-primary-700 border border-primary-200' : 
			'bg-surface-100 text-surface-700 border border-surface-200'
		}`}>
			{#if data.runGraph.run.status === 'running'}
				<RotateCwIcon class="size-4 animate-spin" />
			{:else if data.runGraph.run.status === 'succeeded'}
				<CheckCircle2Icon class="size-4" />
			{:else if data.runGraph.run.status === 'failed'}
				<XCircleIcon class="size-4" />
			{:else}
				<ClockIcon class="size-4" />
			{/if}
			{data.runGraph.run.status}
		</div>
	</div>
</div>

<div class="grid lg:grid-cols-4 gap-6 mb-8">
	<!-- Summary Stats -->
	<div class="lg:col-span-3 grid grid-cols-2 md:grid-cols-4 gap-4">
		<div class="bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-2xl p-4 shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)]">
			<p class="text-xs font-semibold text-surface-500 uppercase tracking-wider mb-2 flex items-center gap-1.5"><CheckCircle2Icon class="size-3.5 text-success-500" /> Succeeded</p>
			<p class="text-2xl font-bold text-surface-900">{data.runGraph.summary.succeeded_nodes}</p>
		</div>
		<div class="bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-2xl p-4 shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)]">
			<p class="text-xs font-semibold text-surface-500 uppercase tracking-wider mb-2 flex items-center gap-1.5"><XCircleIcon class="size-3.5 text-error-500" /> Failed</p>
			<p class="text-2xl font-bold {data.runGraph.summary.failed_nodes > 0 ? 'text-error-600' : 'text-surface-900'}">{data.runGraph.summary.failed_nodes}</p>
		</div>
		<div class="bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-2xl p-4 shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)]">
			<p class="text-xs font-semibold text-surface-500 uppercase tracking-wider mb-2 flex items-center gap-1.5"><RotateCwIcon class="size-3.5 text-warning-500" /> Retrying</p>
			<p class="text-2xl font-bold {data.runGraph.summary.retrying_nodes > 0 ? 'text-warning-600' : 'text-surface-900'}">{data.runGraph.summary.retrying_nodes}</p>
		</div>
		<div class="bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-2xl p-4 shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)]">
			<p class="text-xs font-semibold text-surface-500 uppercase tracking-wider mb-2 flex items-center gap-1.5"><LayersIcon class="size-3.5 text-secondary-500" /> Fan-out Items</p>
			<p class="text-2xl font-bold text-surface-900">{data.runGraph.summary.total_items}</p>
		</div>
	</div>

	<!-- Timing -->
	<div class="bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-2xl p-4 shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)] flex flex-col justify-center gap-3">
		<div>
			<p class="text-[0.65rem] font-bold text-surface-400 uppercase tracking-wider mb-0.5">Started At</p>
			<p class="text-sm font-medium text-surface-800 flex items-center gap-1.5"><ClockIcon class="size-3.5 text-surface-400" /> {formatDateTime(data.runGraph.run.started_at)}</p>
		</div>
		<div class="h-px bg-surface-200/60 w-full"></div>
		<div>
			<p class="text-[0.65rem] font-bold text-surface-400 uppercase tracking-wider mb-0.5">Completed At</p>
			<p class="text-sm font-medium text-surface-800 flex items-center gap-1.5"><CheckCircle2Icon class="size-3.5 text-surface-400" /> {formatDateTime(data.runGraph.run.completed_at) || 'In progress...'}</p>
		</div>
	</div>
</div>

<div class="bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-3xl shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)] overflow-hidden mb-8">
	<div class="p-6 border-b border-surface-200/60 bg-surface-50/50 flex flex-col md:flex-row md:items-center justify-between gap-4">
		<div>
			<h2 class="text-lg font-semibold text-surface-900 flex items-center gap-2">
				<ServerIcon class="size-5 text-secondary-500" />
				Execution DAG
			</h2>
			<p class="text-sm text-surface-500 mt-1">{data.runGraph.definition.title}</p>
		</div>
		
		<div class="flex items-center p-1 bg-surface-200/50 rounded-lg w-fit">
			<button class="px-4 py-1.5 rounded-md text-sm font-medium transition-all {viewMode === 'graph' ? 'bg-white shadow-sm text-surface-900' : 'text-surface-600 hover:text-surface-900'}" onclick={() => viewMode = 'graph'}>Graph View</button>
			<button class="px-4 py-1.5 rounded-md text-sm font-medium transition-all {viewMode === 'list' ? 'bg-white shadow-sm text-surface-900' : 'text-surface-600 hover:text-surface-900'}" onclick={() => viewMode = 'list'}>List View</button>
		</div>
	</div>

	{#if viewMode === 'graph'}
		<div class="p-6 bg-white/30">
			<Graph nodes={data.runGraph.nodes} edges={data.runGraph.edges} />
		</div>
	{:else}
		<div class="p-6">
			<div class="grid gap-4 xl:grid-cols-2">
				{#each data.runGraph.nodes as node}
					<article class="bg-white border border-surface-200 rounded-2xl p-5 shadow-sm hover:shadow-md transition-shadow">
						<div class="flex items-start justify-between gap-4">
							<div>
								<div class="flex items-center gap-2 mb-1.5">
									<span class="bg-surface-100 text-surface-600 px-2 py-0.5 rounded text-[0.65rem] font-bold uppercase tracking-wider">{node.node.kind}</span>
									{#if node.node.queue}
										<span class="text-xs text-surface-500 flex items-center gap-1"><LayersIcon class="size-3" /> {node.node.queue}</span>
									{/if}
								</div>
								<h4 class="text-lg font-semibold text-surface-900">{node.node.label}</h4>
								<p class="font-mono text-xs text-surface-400 mt-1">{node.node.id}</p>
							</div>
							<span class={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[0.65rem] font-bold uppercase tracking-wider ${
								node.status === 'failed' ? 'bg-error-50 text-error-700 ring-1 ring-error-500/20' : 
								node.status === 'succeeded' ? 'bg-success-50 text-success-700 ring-1 ring-success-500/20' : 
								(node.status === 'running' || node.status === 'waiting_retry') ? 'bg-warning-50 text-warning-700 ring-1 ring-warning-500/20' : 
								'bg-surface-100 text-surface-700 ring-1 ring-surface-500/20'
							}`}>
								{node.status}
							</span>
						</div>

						<div class="mt-5 grid gap-3 md:grid-cols-2">
							<div class="rounded-xl bg-surface-50/80 border border-surface-100 p-3">
								<p class="text-[0.65rem] font-bold text-surface-400 uppercase tracking-wider mb-1">Attempts</p>
								<p class="text-sm font-medium text-surface-800">{node.step?.attempt ?? 0} <span class="text-surface-400 font-normal">/ {node.step?.max_attempts ?? node.node.max_attempts ?? 0}</span></p>
							</div>
							<div class="rounded-xl bg-surface-50/80 border border-surface-100 p-3">
								<p class="text-[0.65rem] font-bold text-surface-400 uppercase tracking-wider mb-1">Depends on</p>
								<p class="text-sm font-medium text-surface-800 truncate" title={node.node.depends_on?.join(', ')}>{node.node.depends_on?.join(', ') || 'Root step'}</p>
							</div>
						</div>

						{#if nodeChildren(node).length > 0}
							<div class="mt-4 rounded-xl border border-surface-200 overflow-hidden">
								<div class="flex items-center justify-between bg-surface-50 px-4 py-2 border-b border-surface-200">
									<p class="text-xs font-semibold text-surface-700">Fan-out items</p>
									<p class="text-[0.65rem] font-medium text-surface-500 bg-white px-2 py-0.5 rounded-full border border-surface-200">{node.item_counts.succeeded} / {node.item_counts.total} succeeded</p>
								</div>
								<div class="divide-y divide-surface-100 max-h-48 overflow-y-auto bg-white">
									{#each nodeChildren(node) as item}
										<div class="flex items-center justify-between px-4 py-2.5 hover:bg-surface-50 transition-colors">
											<div class="min-w-0">
												<div class="font-mono text-xs font-medium text-surface-700 truncate">{item.item_key}</div>
												<div class="text-[0.65rem] text-surface-400 mt-0.5">Attempt {item.attempt}/{item.max_attempts}</div>
											</div>
											<span class={`inline-block px-2 py-0.5 rounded text-[0.65rem] font-bold uppercase tracking-wider ${
												item.status === 'failed' ? 'text-error-600 bg-error-50' : 
												item.status === 'succeeded' ? 'text-success-600 bg-success-50' : 
												'text-surface-600 bg-surface-100'
											}`}>{item.status}</span>
										</div>
									{/each}
								</div>
							</div>
						{/if}

						{#if node.step}
							<div class="mt-4 grid gap-3 xl:grid-cols-2">
								<div class="flex flex-col">
									<p class="mb-1.5 text-[0.65rem] font-bold text-surface-400 uppercase tracking-wider">Input</p>
									<div class="flex-1 bg-surface-900 rounded-xl overflow-hidden shadow-inner border border-surface-800">
										<pre class="p-3 text-[0.7rem] text-surface-300 font-mono overflow-x-auto m-0 leading-relaxed">{prettyJSON(node.step.input_json)}</pre>
									</div>
								</div>
								<div class="flex flex-col">
									<p class="mb-1.5 text-[0.65rem] font-bold text-surface-400 uppercase tracking-wider">Output / Error</p>
									<div class="flex-1 bg-surface-900 rounded-xl overflow-hidden shadow-inner border border-surface-800">
										<pre class="p-3 text-[0.7rem] text-surface-300 font-mono overflow-x-auto m-0 leading-relaxed">{prettyJSON(node.step.error_json ? node.step.error_json : node.step.output_json)}</pre>
									</div>
								</div>
							</div>
						{/if}
					</article>
				{/each}
			</div>
		</div>
	{/if}
</div>