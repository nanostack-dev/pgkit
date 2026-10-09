<script lang="ts">
	import { invalidateAll } from '$app/navigation';
	import { onMount } from 'svelte';
	import { cancelWorkflowRun, retryWorkflowRun } from '$lib/api';
	import { formatDateTime, formatRelative, prettyJSON } from '$lib/format';
	import { isFinished, isRetryable, statusBadgeClass, statusLabel } from '$lib/status';
	import type { WorkflowRunDetail, WorkflowStep, WorkflowStepKind } from '$lib/types';
	import {
		ArrowLeftIcon,
		CheckCircle2Icon,
		XCircleIcon,
		ClockIcon,
		RotateCwIcon,
		CircleSlashIcon,
		CodeIcon,
		MoonIcon,
		MailIcon,
		GitBranchIcon,
		AlertTriangleIcon,
		HourglassIcon
	} from 'lucide-svelte';

	let { data } = $props<{ data: { detail: WorkflowRunDetail } }>();
	let busy = $state(false);
	let actionError = $state('');
	let expanded = $state<Record<string, boolean>>({});

	const run = $derived(data.detail.run);
	const steps = $derived(data.detail.steps as WorkflowStep[]);
	const children = $derived(data.detail.children);
	const counts = $derived({
		succeeded: steps.filter((step: WorkflowStep) => step.status === 'succeeded').length,
		waiting: steps.filter((step: WorkflowStep) => step.status === 'waiting' || step.status === 'retrying').length,
		failed: steps.filter((step: WorkflowStep) => step.status === 'failed' || step.status === 'timed_out').length,
		attempts: steps.reduce((total: number, step: WorkflowStep) => total + step.attempts, 0)
	});

	const kindIcons: Record<WorkflowStepKind, typeof CodeIcon> = {
		step: CodeIcon,
		sleep: MoonIcon,
		signal: MailIcon,
		child: GitBranchIcon
	};

	function stepDetail(step: WorkflowStep): string {
		switch (step.kind) {
			case 'sleep':
				return step.status === 'waiting' ? `wakes ${formatRelative(step.wake_at)}` : 'slept';
			case 'signal':
				if (step.status === 'waiting') {
					return step.wake_at ? `waits for "${step.signal}" until ${formatDateTime(step.wake_at)}` : `waits for "${step.signal}"`;
				}
				return step.status === 'timed_out' ? `no "${step.signal}" arrived in time` : `received "${step.signal}"`;
			case 'child':
				return step.status === 'waiting' ? 'child run in progress' : 'child run finished';
			default:
				if (step.status === 'retrying') {
					return `attempt ${step.attempts} failed, retries ${formatRelative(step.wake_at)}`;
				}
				return `${step.attempts} attempt${step.attempts === 1 ? '' : 's'}`;
		}
	}

	async function act(action: (runID: string) => Promise<unknown>, failure: string) {
		busy = true;
		actionError = '';
		try {
			await action(run.id);
			await invalidateAll();
		} catch (err) {
			actionError = err instanceof Error ? err.message : failure;
		} finally {
			busy = false;
		}
	}

	onMount(() => {
		const timer = window.setInterval(() => {
			if (!isFinished(run.status)) {
				void invalidateAll();
			}
		}, 3000);
		return () => window.clearInterval(timer);
	});
</script>

<div class="mb-6">
	<a href="/workflows" class="inline-flex items-center gap-2 text-sm font-medium text-surface-500 hover:text-surface-900 transition-colors mb-4">
		<ArrowLeftIcon class="size-4" /> Back to Workflows
	</a>
	<div class="flex flex-col md:flex-row md:items-start justify-between gap-4">
		<div class="min-w-0">
			<div class="flex items-center gap-3 mb-1">
				<h1 class="text-3xl font-semibold tracking-tight text-surface-900 truncate">{run.workflow}</h1>
				<span class="bg-surface-100 text-surface-600 px-2 py-0.5 rounded-md text-xs font-bold font-mono">v{run.version}</span>
			</div>
			<p class="font-mono text-sm text-surface-500 flex flex-wrap items-center gap-2">
				<span class="break-all">{run.id}</span>
				{#if run.key}
					<span class="text-surface-300">•</span>
					<span class="text-primary-600 bg-primary-50 px-1.5 py-0.5 rounded break-all">{run.key}</span>
				{/if}
				{#if run.parent_run_id}
					<span class="text-surface-300">•</span>
					<a href={`/workflows/${run.parent_run_id}`} class="text-secondary-600 hover:text-secondary-800 underline-offset-2 hover:underline">parent run</a>
				{/if}
			</p>
		</div>
		<div class="flex items-center gap-3 shrink-0">
			{#if isRetryable(run.status)}
				<button
					class="inline-flex items-center gap-2 rounded-xl border border-surface-200 bg-white px-4 py-2 text-sm font-medium text-surface-700 transition-colors hover:bg-surface-50 disabled:opacity-50"
					onclick={() => act(retryWorkflowRun, 'Failed to retry run.')}
					disabled={busy}
				>
					<RotateCwIcon class={`size-4 ${busy ? 'animate-spin' : ''}`} />
					Retry from checkpoints
				</button>
			{:else if !isFinished(run.status)}
				<button
					class="inline-flex items-center gap-2 rounded-xl border border-surface-200 bg-white px-4 py-2 text-sm font-medium text-surface-700 transition-colors hover:bg-surface-50 disabled:opacity-50"
					onclick={() => act(cancelWorkflowRun, 'Failed to cancel run.')}
					disabled={busy}
				>
					<CircleSlashIcon class="size-4" />
					Cancel run
				</button>
			{/if}
			<span class={`inline-flex items-center gap-2 px-4 py-2 rounded-xl text-sm font-bold uppercase tracking-wider ${statusBadgeClass(run.status)}`}>
				{#if run.status === 'running'}
					<RotateCwIcon class="size-4 animate-spin" />
				{:else if run.status === 'succeeded'}
					<CheckCircle2Icon class="size-4" />
				{:else if run.status === 'failed'}
					<XCircleIcon class="size-4" />
				{:else if run.status === 'waiting'}
					<HourglassIcon class="size-4" />
				{:else}
					<ClockIcon class="size-4" />
				{/if}
				{run.status}
			</span>
		</div>
	</div>
</div>

{#if actionError}
	<div class="mb-6 rounded-xl border border-error-200 bg-error-50 px-4 py-3 text-sm text-error-800">{actionError}</div>
{/if}

{#if run.error && run.status !== 'cancelled'}
	<div class="mb-6 rounded-2xl border border-error-200 bg-error-50 p-4 text-sm text-error-800 flex gap-3">
		<AlertTriangleIcon class="size-5 text-error-600 shrink-0 mt-0.5" />
		<div class="min-w-0">
			<p class="font-semibold">{run.timed_out ? 'The run outlived its timeout' : 'The run failed'}</p>
			<p class="mt-1 font-mono text-xs break-words">{run.error}</p>
		</div>
	</div>
{/if}

<div class="grid lg:grid-cols-4 gap-6 mb-8">
	<div class="lg:col-span-3 grid grid-cols-2 md:grid-cols-4 gap-4">
		<div class="bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-2xl p-4 shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)]">
			<p class="text-xs font-semibold text-surface-500 uppercase tracking-wider mb-2 flex items-center gap-1.5"><CheckCircle2Icon class="size-3.5 text-success-500" /> Completed</p>
			<p class="text-2xl font-bold text-surface-900">{counts.succeeded}</p>
		</div>
		<div class="bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-2xl p-4 shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)]">
			<p class="text-xs font-semibold text-surface-500 uppercase tracking-wider mb-2 flex items-center gap-1.5"><HourglassIcon class="size-3.5 text-secondary-500" /> Waiting</p>
			<p class="text-2xl font-bold text-surface-900">{counts.waiting}</p>
		</div>
		<div class="bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-2xl p-4 shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)]">
			<p class="text-xs font-semibold text-surface-500 uppercase tracking-wider mb-2 flex items-center gap-1.5"><XCircleIcon class="size-3.5 text-error-500" /> Failed</p>
			<p class="text-2xl font-bold {counts.failed > 0 ? 'text-error-600' : 'text-surface-900'}">{counts.failed}</p>
		</div>
		<div class="bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-2xl p-4 shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)]">
			<p class="text-xs font-semibold text-surface-500 uppercase tracking-wider mb-2 flex items-center gap-1.5"><RotateCwIcon class="size-3.5 text-warning-500" /> Attempts</p>
			<p class="text-2xl font-bold text-surface-900">{counts.attempts}</p>
		</div>
	</div>

	<div class="bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-2xl p-4 shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)] flex flex-col justify-center gap-2 text-sm">
		<p class="flex justify-between gap-2"><span class="text-surface-500">Created</span><span class="font-medium text-surface-800">{formatDateTime(run.created_at)}</span></p>
		<p class="flex justify-between gap-2"><span class="text-surface-500">Completed</span><span class="font-medium text-surface-800">{run.completed_at ? formatDateTime(run.completed_at) : 'In progress'}</span></p>
		{#if run.wake_at}
			<p class="flex justify-between gap-2"><span class="text-surface-500">Wakes</span><span class="font-medium text-secondary-700">{formatRelative(run.wake_at)}</span></p>
		{/if}
		{#if run.deadline_at}
			<p class="flex justify-between gap-2"><span class="text-surface-500">Deadline</span><span class="font-medium text-surface-800">{formatDateTime(run.deadline_at)}</span></p>
		{/if}
	</div>
</div>

<div class="bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-3xl shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)] overflow-hidden mb-8">
	<div class="p-6 border-b border-surface-200/60 bg-surface-50/50">
		<h2 class="text-lg font-semibold text-surface-900">Checkpoints</h2>
		<p class="text-sm text-surface-500 mt-1">Durable operations in the order the run first reached them. Replays return these results instead of running again.</p>
	</div>
	{#if steps.length === 0}
		<p class="p-6 text-sm text-surface-500">The run has not recorded a checkpoint yet.</p>
	{:else}
		<ol class="divide-y divide-surface-200/50">
			{#each steps as step (step.name)}
				{@const Icon = kindIcons[step.kind]}
				<li class="px-6 py-4">
					<div class="flex items-start gap-4">
						<div class="bg-surface-100 text-surface-600 p-2 rounded-xl shrink-0"><Icon class="size-4" /></div>
						<div class="min-w-0 flex-1">
							<div class="flex flex-wrap items-center gap-2">
								<span class="font-mono text-sm font-semibold text-surface-900 break-all">{step.name}</span>
								<span class="text-[0.65rem] font-bold uppercase tracking-wider text-surface-400">{step.kind}</span>
								<span class={`inline-flex px-2 py-0.5 rounded-full text-[0.65rem] font-bold uppercase tracking-wider ${statusBadgeClass(step.status)}`}>{statusLabel(step.status)}</span>
							</div>
							<p class="text-xs text-surface-500 mt-1">{stepDetail(step)}</p>
							{#if step.error}
								<p class="mt-2 font-mono text-xs text-error-700 bg-error-50 rounded-lg px-3 py-2 break-words">{step.error}</p>
							{/if}
							{#if step.child_run_id}
								<a href={`/workflows/${step.child_run_id}`} class="mt-2 inline-block font-mono text-xs text-secondary-600 hover:text-secondary-800 hover:underline break-all">{step.child_run_id}</a>
							{/if}
							{#if step.output}
								<button class="mt-2 block text-xs font-medium text-surface-500 hover:text-surface-800" onclick={() => (expanded[step.name] = !expanded[step.name])}>
									{expanded[step.name] ? 'Hide result' : 'Show result'}
								</button>
								{#if expanded[step.name]}
									<pre class="mt-2 p-3 text-[0.7rem] text-surface-300 bg-surface-900 rounded-xl font-mono overflow-x-auto leading-relaxed">{prettyJSON(step.output)}</pre>
								{/if}
							{/if}
						</div>
						<span class="text-xs text-surface-400 shrink-0">{formatDateTime(step.created_at)}</span>
					</div>
				</li>
			{/each}
		</ol>
	{/if}
</div>

{#if children.length > 0}
	<div class="bg-white/70 backdrop-blur-xl border border-surface-200/60 rounded-3xl shadow-[0_4px_20px_-8px_rgba(0,0,0,0.05)] overflow-hidden mb-8">
		<div class="p-6 border-b border-surface-200/60 bg-surface-50/50">
			<h2 class="text-lg font-semibold text-surface-900">Child runs</h2>
		</div>
		<ul class="divide-y divide-surface-200/50">
			{#each children as child (child.id)}
				<li class="px-6 py-3 flex items-center justify-between gap-4">
					<a href={`/workflows/${child.id}`} class="min-w-0 hover:underline">
						<span class="font-medium text-surface-900">{child.workflow}</span>
						<span class="ml-2 font-mono text-xs text-surface-500 break-all">{child.key}</span>
					</a>
					<span class={`inline-flex px-2 py-0.5 rounded-full text-[0.65rem] font-bold uppercase tracking-wider ${statusBadgeClass(child.status)}`}>{child.status}</span>
				</li>
			{/each}
		</ul>
	</div>
{/if}

<div class="grid gap-6 xl:grid-cols-2">
	<div>
		<p class="mb-1.5 text-[0.65rem] font-bold text-surface-400 uppercase tracking-wider">Input</p>
		<pre class="p-4 text-[0.7rem] text-surface-300 bg-surface-900 rounded-2xl font-mono overflow-x-auto leading-relaxed">{prettyJSON(run.input)}</pre>
	</div>
	<div>
		<p class="mb-1.5 text-[0.65rem] font-bold text-surface-400 uppercase tracking-wider">Output</p>
		<pre class="p-4 text-[0.7rem] text-surface-300 bg-surface-900 rounded-2xl font-mono overflow-x-auto leading-relaxed">{run.output ? prettyJSON(run.output) : 'No output yet'}</pre>
	</div>
</div>
