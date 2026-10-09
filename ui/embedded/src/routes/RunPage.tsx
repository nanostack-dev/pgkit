import { Ban, ChevronLeft, ChevronRight, RotateCcw, SearchX, TimerOff } from 'lucide-react';
import { useState } from 'react';
import { Link, useParams } from 'react-router';
import { toast } from 'sonner';
import { ApiError, errorMessage } from '../api/client';
import { useCancelRun, useConfig, useRetryRun, useRun } from '../api/queries';
import type { RunDetail, RunSignal } from '../api/types';
import { Button } from '../components/Button';
import { CopyButton, IdText } from '../components/Copy';
import { ConfirmDialog } from '../components/Dialogs';
import { jobHref } from '../lib/links';
import { JsonView } from '../components/JsonView';
import { Fact, LiveIndicator, Section } from '../components/Layout';
import { EmptyState, ErrorState, Skeleton } from '../components/States';
import { StatusBadge, StatusDot } from '../components/StatusBadge';
import { Countdown, Elapsed, RelativeTime } from '../components/Time';
import { cn } from '../lib/cn';
import { formatDateTime, jobLabel, plural, shortId } from '../lib/format';
import { runFinished, runRetryable } from '../lib/status';
import { RunTreeView } from './run/RunTreeView';
import { Timeline } from './run/Timeline';

function Breadcrumbs({ detail }: { detail: RunDetail }) {
	const ancestors = detail.ancestors;
	if (ancestors.length === 0) {
		return (
			<Link to="/workflows" className="inline-flex min-h-9 items-center gap-1 text-[0.8125rem] text-muted hover:text-fg">
				<ChevronLeft aria-hidden className="size-4" />
				Workflows
			</Link>
		);
	}
	const shown = ancestors.length > 3 ? [ancestors[0]!, null, ...ancestors.slice(-2)] : ancestors;
	return (
		<nav aria-label="Parent runs" className="no-scrollbar -mx-4 overflow-x-auto px-4">
			<ol className="flex min-h-9 items-center gap-1 text-[0.8125rem] whitespace-nowrap text-muted">
				<li>
					<Link to="/workflows" className="hover:text-fg">
						Workflows
					</Link>
				</li>
				{shown.map((ancestor, index) => (
					<li key={ancestor?.id ?? `gap-${index}`} className="flex items-center gap-1">
						<ChevronRight aria-hidden className="size-3.5 text-subtle" />
						{ancestor ? (
							<Link to={`/workflows/${encodeURIComponent(ancestor.id)}`} className="inline-flex items-center gap-1.5 hover:text-fg">
								<StatusDot status={ancestor.status} />
								<span className="max-w-[10rem] truncate">{ancestor.key ?? ancestor.workflow}</span>
							</Link>
						) : (
							<span aria-label={`${ancestors.length - 3} more parent runs`}>…</span>
						)}
					</li>
				))}
			</ol>
		</nav>
	);
}

function RunActions({ detail, mutationsEnabled }: { detail: RunDetail; mutationsEnabled: boolean }) {
	const [confirm, setConfirm] = useState<'retry' | 'cancel' | null>(null);
	const retry = useRetryRun();
	const cancel = useCancelRun();
	const run = detail.run;
	const finished = runFinished(run.status);
	const retryable = runRetryable(run.status);
	const label = run.key ?? shortId(run.id);

	if (!retryable && finished) return null;
	if (!mutationsEnabled) {
		return <p className="text-xs text-muted">Read-only server: retry and cancel are disabled.</p>;
	}

	return (
		<>
			<div className="flex gap-2">
				{retryable ? (
					<Button variant="primary" icon={RotateCcw} className="flex-1 sm:flex-none" onClick={() => setConfirm('retry')}>
						Retry run
					</Button>
				) : null}
				{!finished ? (
					<Button variant="danger-outline" icon={Ban} className="flex-1 sm:flex-none" onClick={() => setConfirm('cancel')}>
						Cancel run
					</Button>
				) : null}
			</div>
			<ConfirmDialog
				open={confirm === 'retry'}
				onOpenChange={(open) => setConfirm(open ? 'retry' : null)}
				title={`Retry ${run.workflow} ${label}?`}
				description="The run resumes from its checkpoints: succeeded steps keep their recorded results and are not executed again. Failed steps and everything after them run again."
				confirmLabel="Retry run"
				pending={retry.isPending}
				onConfirm={() =>
					retry.mutate(run.id, {
						onSuccess: () => {
							setConfirm(null);
							toast.success('Run is pending again and will resume from its checkpoints.');
						},
						onError: (error) => toast.error(errorMessage(error)),
					})
				}
			/>
			<ConfirmDialog
				open={confirm === 'cancel'}
				onOpenChange={(open) => setConfirm(open ? 'cancel' : null)}
				title={`Cancel ${run.workflow} ${label}?`}
				description={
					detail.child_total > 0
						? `The run stops at its next durable operation and its unfinished child runs are cancelled too (${plural(detail.child_total, 'child run', 'child runs')}). A cancelled run can be retried later.`
						: 'The run stops at its next durable operation. A cancelled run can be retried later.'
				}
				confirmLabel="Cancel run"
				tone="danger"
				pending={cancel.isPending}
				onConfirm={() =>
					cancel.mutate(run.id, {
						onSuccess: () => {
							setConfirm(null);
							toast.success('Run cancelled.');
						},
						onError: (error) => toast.error(errorMessage(error)),
					})
				}
			/>
		</>
	);
}

function SignalList({ signals }: { signals: RunSignal[] }) {
	return (
		<ul className="divide-y divide-line">
			{signals.map((signal) => (
				<li key={signal.id} className="flex flex-col gap-1 py-2.5 text-[0.8125rem] first:pt-0 last:pb-0">
					<span className="flex min-w-0 items-center gap-2">
						<span className="truncate font-mono font-medium">{signal.name}</span>
						<span
							className={cn(
								'shrink-0 rounded-full px-2 py-0.5 text-[0.6875rem] font-medium',
								signal.received ? 'bg-ok-soft text-ok' : 'bg-wait-soft text-wait',
							)}
						>
							{signal.received ? 'Received' : 'Buffered'}
						</span>
						<RelativeTime value={signal.created_at} className="ml-auto shrink-0 text-xs text-muted" />
					</span>
					{signal.payload_preview ? (
						<code className="block truncate font-mono text-xs text-muted" title={signal.payload_preview}>
							{signal.payload_preview}
						</code>
					) : null}
				</li>
			))}
		</ul>
	);
}

function RunSkeleton() {
	return (
		<div role="status" aria-label="Loading run" className="space-y-5">
			<Skeleton className="h-4 w-24" />
			<div className="space-y-2">
				<Skeleton className="h-7 w-56" />
				<Skeleton className="h-4 w-40" />
			</div>
			<div className="card space-y-5 p-4">
				{Array.from({ length: 4 }, (_, index) => (
					<div key={index} className="flex gap-3">
						<Skeleton className="size-8 rounded-full" />
						<div className="flex-1 space-y-2">
							<Skeleton className="h-4 w-1/3" />
							<Skeleton className="h-3 w-1/2" />
						</div>
					</div>
				))}
			</div>
		</div>
	);
}

export function RunPage() {
	const { runId = '' } = useParams();
	const run = useRun(runId);
	const config = useConfig();
	const mutationsEnabled = config.data?.mutations_enabled ?? false;

	if (run.isPending) return <RunSkeleton />;

	if (run.isError && !run.data) {
		const notFound = run.error instanceof ApiError && run.error.status === 404;
		return (
			<div className="card">
				{notFound ? (
					<EmptyState
						icon={SearchX}
						title="Run not found"
						action={
							<Link to="/workflows" className="text-[0.8125rem] font-medium text-fg underline underline-offset-4">
								Back to workflows
							</Link>
						}
					>
						No run has the ID <code className="font-mono [overflow-wrap:anywhere]">{runId}</code>. It may have been purged after it finished.
					</EmptyState>
				) : (
					<ErrorState error={run.error} onRetry={() => run.refetch()} />
				)}
			</div>
		);
	}

	const detail = run.data!;
	const info = detail.run;
	const finished = runFinished(info.status);
	const hasTree = detail.child_total > 0;

	return (
		<>
			<Breadcrumbs detail={detail} />
			<header className="mt-2 mb-5 md:mb-6">
				<div className="flex flex-wrap items-center gap-2">
					<StatusBadge status={info.status} />
					{finished ? null : <LiveIndicator updatedAt={run.dataUpdatedAt} error={run.isError} />}
					{info.version > 0 ? <span className="tabular text-xs text-subtle">version {info.version}</span> : null}
				</div>
				<h1 className="mt-2 line-clamp-2 text-xl font-semibold tracking-[-0.01em] [overflow-wrap:anywhere] md:text-2xl" title={info.workflow}>
					{info.workflow}
				</h1>
				<div className="mt-1 flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1">
					{info.key ? (
						<span className="inline-flex min-w-0 items-center gap-0.5">
							<code className="line-clamp-2 min-w-0 font-mono text-[0.8125rem] text-fg [overflow-wrap:anywhere]" title={info.key}>
								{info.key}
							</code>
							<CopyButton value={info.key} label="Copy key" />
						</span>
					) : null}
					<IdText id={info.id} />
				</div>
				<div className="mt-4 empty:hidden">
					<RunActions detail={detail} mutationsEnabled={mutationsEnabled} />
				</div>
			</header>

			{info.error ? (
				<div role="alert" className="mb-5 overflow-hidden rounded-xl border border-bad/30 bg-bad-soft">
					<div className="flex items-start gap-2.5 px-4 py-3">
						{info.timed_out ? <TimerOff aria-hidden className="mt-0.5 size-4 shrink-0 text-bad" /> : null}
						<div className="min-w-0 flex-1">
							<p className="text-[0.8125rem] font-medium text-bad">
								{info.timed_out ? 'Run timed out' : info.status === 'cancelled' ? 'Run cancelled' : 'Run failed'}
							</p>
							<pre className="mt-1 max-h-48 overflow-auto overscroll-contain font-mono text-[0.75rem] leading-relaxed whitespace-pre-wrap text-fg [overflow-wrap:anywhere]">
								{info.error}
							</pre>
						</div>
						<CopyButton value={info.error} label="Copy error" />
					</div>
				</div>
			) : null}

			<div className="grid gap-5 lg:grid-cols-[minmax(0,1fr)_20rem] lg:gap-6 xl:grid-cols-[minmax(0,1fr)_22rem]">
				<div className="flex min-w-0 flex-col gap-5 lg:gap-6">
					<Section
						title="Checkpoints"
						description={plural(detail.steps.length, 'durable operation', 'durable operations')}
						className="lg:order-none"
					>
						<Timeline steps={detail.steps} children={detail.children} runFinished={finished} />
					</Section>
					<Section title="Input">
						<JsonView value={info.input} label="Input" emptyLabel="No input" />
					</Section>
					<Section title="Output">
						<JsonView
							value={info.output}
							label="Output"
							emptyLabel={finished ? 'The run produced no output.' : 'Available once the run succeeds.'}
						/>
					</Section>
				</div>

				<div className="flex min-w-0 flex-col gap-5 lg:gap-6">
					<Section title="Details">
						<dl className="grid grid-cols-2 gap-x-4 gap-y-3.5">
							<Fact label="Created">
								<RelativeTime value={info.created_at} />
							</Fact>
							<Fact label="Started">
								<RelativeTime value={info.started_at} />
							</Fact>
							<Fact label={finished ? 'Completed' : 'Updated'}>
								<RelativeTime value={finished ? info.completed_at : info.updated_at} />
							</Fact>
							<Fact label={finished ? 'Duration' : 'Elapsed'}>
								<Elapsed from={info.started_at ?? info.created_at} to={finished ? info.completed_at : null} />
							</Fact>
							{info.wake_at && !finished ? (
								<Fact label="Wakes">
									<Countdown value={info.wake_at} prefix="in" overdue="now" />
								</Fact>
							) : null}
							{info.deadline_at ? (
								<Fact label="Deadline">
									{finished ? (
										<span title={formatDateTime(info.deadline_at)}>{formatDateTime(info.deadline_at)}</span>
									) : (
										<Countdown value={info.deadline_at} prefix="in" overdue="passed" />
									)}
								</Fact>
							) : null}
							{detail.job_id !== null ? (
								<Fact label="Queue job">
									<Link to={jobHref(detail.job_id)} className="tabular font-mono underline-offset-4 hover:underline">
										{jobLabel(detail.job_id)}
									</Link>
								</Fact>
							) : null}
							{info.key ? (
								<Fact label="Key" className="col-span-2">
									<code className="font-mono text-[0.8125rem] [overflow-wrap:anywhere]">{info.key}</code>
								</Fact>
							) : null}
							<Fact label="Run ID" className="col-span-2">
								<IdText id={info.id} full />
							</Fact>
						</dl>
					</Section>
					{hasTree ? (
						<Section
							title="Run tree"
							description={`${plural(detail.child_total, 'child run', 'child runs')} and their descendants`}
						>
							<RunTreeView runId={info.id} fallback={detail.children} live={!finished} />
						</Section>
					) : null}
					{detail.signals.length > 0 ? (
						<Section title="Signals" description="Delivered to this run">
							<SignalList signals={detail.signals} />
						</Section>
					) : null}
				</div>
			</div>
		</>
	);
}
