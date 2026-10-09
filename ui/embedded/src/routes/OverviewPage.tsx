import NumberFlow from '@number-flow/react';
import { Toggle } from '@base-ui/react/toggle';
import { ToggleGroup } from '@base-ui/react/toggle-group';
import { Activity, CircleCheck, Hourglass, Inbox, Layers, TriangleAlert, Workflow } from 'lucide-react';
import { useState } from 'react';
import { Link } from 'react-router';
import { useOverview } from '../api/queries';
import type { JobBucket, Overview, OverviewRange, QueueJob, RunBucket, RunSummary } from '../api/types';
import { jobHref } from '../lib/links';
import { LiveIndicator, PageHeader, Section, SectionLink } from '../components/Layout';
import { MiddleTruncate } from '../components/MiddleTruncate';
import { RunIdentity, RunProgress } from '../components/RunRow';
import { StatusBadge } from '../components/StatusBadge';
import { EmptyState, ErrorState, Skeleton } from '../components/States';
import { ThroughputChart, type SeriesDef } from '../components/ThroughputChart';
import { RelativeTime } from '../components/Time';
import { useNow } from '../lib/clock';
import { cn } from '../lib/cn';
import { firstLine, formatDuration, formatNumber, parseTime, plural, jobLabel } from '../lib/format';

const jobSeries: SeriesDef<JobBucket>[] = [
	{ key: 'done', label: 'Done', barClass: 'bg-fg/75', dotClass: 'bg-fg/75' },
	{ key: 'failed', label: 'Failed', barClass: 'bg-bad', dotClass: 'bg-bad' },
];

const runSeries: SeriesDef<RunBucket>[] = [
	{ key: 'succeeded', label: 'Succeeded', barClass: 'bg-ok', dotClass: 'bg-ok' },
	{ key: 'failed', label: 'Failed', barClass: 'bg-bad', dotClass: 'bg-bad' },
	{ key: 'cancelled', label: 'Cancelled', barClass: 'bg-idle', dotClass: 'bg-idle' },
];

function bucketLabel(seconds: number): string {
	if (seconds === 60) return 'minute';
	if (seconds === 3600) return 'hour';
	return seconds % 60 === 0 ? `${seconds / 60} minutes` : `${seconds} seconds`;
}

function StatTile({
	to,
	label,
	value,
	detail,
	alert = false,
}: {
	to: string;
	label: string;
	value: number;
	detail?: string;
	alert?: boolean;
}) {
	return (
		<Link
			to={to}
			className={cn(
				'press card flex min-w-0 flex-col gap-1.5 p-3.5 transition-[background-color,border-color,transform] hover:bg-surface-2 md:p-4',
				alert && 'border-bad/40',
			)}
		>
			<span className="flex items-center gap-1.5 text-xs font-medium text-muted">
				{alert ? <span aria-hidden className="size-1.5 rounded-full bg-bad" /> : null}
				{label}
			</span>
			<span className="min-w-0">
				<NumberFlow
					value={value}
					className={cn('tabular block text-2xl font-semibold tracking-[-0.02em] md:text-[1.75rem]', alert ? 'text-bad' : 'text-fg')}
				/>
				<span aria-hidden={!detail} className="mt-0.5 block truncate text-xs text-muted">
					{detail ?? '\u00a0'}
				</span>
			</span>
		</Link>
	);
}

function RangeToggle({ value, onChange }: { value: OverviewRange; onChange: (range: OverviewRange) => void }) {
	return (
		<ToggleGroup
			aria-label="Time range"
			value={[value]}
			onValueChange={(next) => {
				const selected = next.find((item) => item !== value);
				if (selected === '1h' || selected === '24h') onChange(selected);
			}}
			className="inline-flex rounded-lg border border-line bg-surface p-0.5"
		>
			{(['1h', '24h'] as const).map((range) => (
				<Toggle
					key={range}
					value={range}
					className="tabular inline-flex h-9 min-w-12 items-center justify-center rounded-md px-2.5 text-xs font-medium text-muted transition-colors select-none hover:text-fg data-[pressed]:bg-surface-3 data-[pressed]:text-fg md:h-7"
				>
					{range === '1h' ? '1 hour' : '24 hours'}
				</Toggle>
			))}
		</ToggleGroup>
	);
}

function Attention({ data }: { data: Overview }) {
	const now = useNow();
	const items: { to: string; text: string }[] = [];
	const failedRuns = data.workflow?.counts.failed ?? 0;
	if (failedRuns > 0) items.push({ to: '/workflows?status=failed', text: plural(failedRuns, 'failed run', 'failed runs') });
	if (data.queue.summary.failed_jobs > 0)
		items.push({ to: '/queues?status=failed', text: plural(data.queue.summary.failed_jobs, 'failed job', 'failed jobs') });
	const oldestDue = parseTime(data.queue.oldest_due_at);
	if (oldestDue !== null && now - oldestDue > 60_000) {
		items.push({
			to: '/queues?status=pending',
			text: `${plural(data.queue.due_jobs, 'due job', 'due jobs')} waiting up to ${formatDuration(now - oldestDue)}`,
		});
	}
	if (items.length === 0) return null;
	return (
		<div role="status" className="mb-5 flex flex-wrap items-center gap-2 rounded-xl border border-bad/30 bg-bad-soft px-3.5 py-2.5 text-[0.8125rem]">
			<TriangleAlert aria-hidden className="size-4 shrink-0 text-bad" />
			<span className="font-medium text-bad">Needs attention</span>
			{items.map((item) => (
				<Link
					key={item.to}
					to={item.to}
					className="press inline-flex min-h-8 items-center rounded-full bg-surface px-2.5 text-xs font-medium text-fg ring-1 ring-bad/25 hover:bg-surface-2"
				>
					{item.text}
				</Link>
			))}
		</div>
	);
}

function WaitingList({ runs, total }: { runs: RunSummary[]; total: number }) {
	if (runs.length === 0) {
		return (
			<EmptyState icon={Hourglass} title="Nothing is waiting">
				Runs waiting on a signal, a sleep or a child run show up here.
			</EmptyState>
		);
	}
	return (
		<ul className="divide-y divide-line">
			{runs.map((run) => (
				<li key={run.id}>
					<Link
						to={`/workflows/${encodeURIComponent(run.id)}`}
						data-row
						className="flex flex-col gap-1 px-4 py-3 text-[0.8125rem] transition-colors outline-offset-[-2px] hover:bg-surface-2 sm:flex-row sm:items-center sm:gap-4"
					>
						<RunIdentity run={run} className="sm:w-[42%] sm:shrink-0" />
						<RunProgress run={run} className="min-w-0 flex-1" />
					</Link>
				</li>
			))}
			{total > runs.length ? (
				<li className="px-4 py-2.5 text-xs text-muted">
					<Link to="/workflows?status=waiting&children=1" className="font-medium hover:text-fg">
						{plural(total - runs.length, 'more waiting run', 'more waiting runs')}
					</Link>
				</li>
			) : null}
		</ul>
	);
}

function FailureList({ runs, jobs }: { runs: RunSummary[]; jobs: QueueJob[] }) {
	if (runs.length === 0 && jobs.length === 0) {
		return (
			<EmptyState icon={CircleCheck} title="No recent failures">
				Failed runs and jobs appear here with their last error.
			</EmptyState>
		);
	}
	return (
		<ul className="divide-y divide-line">
			{runs.map((run) => (
				<li key={run.id}>
					<Link
						to={`/workflows/${encodeURIComponent(run.id)}`}
						data-row
						className="flex flex-col gap-1 px-4 py-3 text-[0.8125rem] transition-colors outline-offset-[-2px] hover:bg-surface-2"
					>
						<span className="flex min-w-0 items-center gap-2">
							<Workflow aria-hidden className="size-3.5 shrink-0 text-subtle" />
							<span className="max-w-[60%] shrink-0 truncate font-medium">{run.workflow}</span>
							<span className="min-w-0 truncate font-mono text-xs text-muted">{run.key ?? ''}</span>
							<RelativeTime value={run.completed_at ?? run.updated_at} className="ml-auto shrink-0 text-xs text-muted" />
						</span>
						<span className="line-clamp-2 text-bad [overflow-wrap:anywhere]">{firstLine(run.error ?? 'Failed', 220)}</span>
					</Link>
				</li>
			))}
			{jobs.map((job) => (
				<li key={job.id}>
					<Link
						to={jobHref(job.id)}
						data-row
						className="flex flex-col gap-1 px-4 py-3 text-[0.8125rem] transition-colors outline-offset-[-2px] hover:bg-surface-2"
					>
						<span className="flex min-w-0 items-center gap-2">
							<Layers aria-hidden className="size-3.5 shrink-0 text-subtle" />
							<MiddleTruncate text={job.queue_name} tail={10} className="font-mono" />
							<span className="tabular shrink-0 font-mono text-xs text-muted">{jobLabel(job.id)}</span>
							<RelativeTime value={job.updated_at} className="ml-auto shrink-0 text-xs text-muted" />
						</span>
						<span className="line-clamp-2 text-bad [overflow-wrap:anywhere]">{firstLine(job.last_error ?? 'Failed', 220)}</span>
					</Link>
				</li>
			))}
		</ul>
	);
}

type ActivityItem =
	| { type: 'run'; at: string; run: RunSummary }
	| { type: 'job'; at: string; job: QueueJob };

function ActivityFeed({ data }: { data: Overview }) {
	const items: ActivityItem[] = [
		...(data.workflow?.recent ?? []).map((run) => ({ type: 'run' as const, at: run.updated_at, run })),
		...data.queue.recent_jobs.map((job) => ({ type: 'job' as const, at: job.updated_at, job })),
	]
		.sort((a, b) => Date.parse(b.at) - Date.parse(a.at))
		.slice(0, 12);

	if (items.length === 0) {
		return (
			<EmptyState icon={Inbox} title="No activity yet">
				Enqueue a job or start a workflow run and it will appear here.
			</EmptyState>
		);
	}
	return (
		<ol className="divide-y divide-line">
			{items.map((item) =>
				item.type === 'run' ? (
					<li key={`run-${item.run.id}`}>
						<Link
							to={`/workflows/${encodeURIComponent(item.run.id)}`}
							data-row
							className="flex items-center gap-3 px-4 py-2.5 text-[0.8125rem] transition-colors outline-offset-[-2px] hover:bg-surface-2"
						>
							<Workflow aria-hidden className="size-3.5 shrink-0 text-subtle" />
							<span className="min-w-0 flex-1 truncate">
								<span className="font-medium">{item.run.workflow}</span>{' '}
								<span className="font-mono text-xs text-muted">{item.run.key ?? ''}</span>
							</span>
							<StatusBadge status={item.run.status} size="sm" />
							<RelativeTime value={item.at} className="w-16 shrink-0 text-right text-xs text-muted" />
						</Link>
					</li>
				) : (
					<li key={`job-${item.job.id}`}>
						<Link
							to={jobHref(item.job.id)}
							data-row
							className="flex items-center gap-3 px-4 py-2.5 text-[0.8125rem] transition-colors outline-offset-[-2px] hover:bg-surface-2"
						>
							<Layers aria-hidden className="size-3.5 shrink-0 text-subtle" />
							<span className="flex min-w-0 flex-1 items-baseline gap-1.5">
								<MiddleTruncate text={item.job.queue_name} tail={10} className="font-mono text-[0.8125rem]" />
								<span className="tabular shrink-0 font-mono text-xs text-muted">{jobLabel(item.job.id)}</span>
							</span>
							<StatusBadge status={item.job.status} size="sm" />
							<RelativeTime value={item.at} className="w-16 shrink-0 text-right text-xs text-muted" />
						</Link>
					</li>
				),
			)}
		</ol>
	);
}

function OverviewSkeleton() {
	return (
		<div role="status" aria-label="Loading overview" className="space-y-5">
			<div className="grid grid-cols-2 gap-3 md:grid-cols-4">
				{Array.from({ length: 8 }, (_, index) => (
					<div key={index} className="card flex min-h-24 flex-col justify-between p-4">
						<Skeleton className="h-3 w-20" />
						<Skeleton className="h-7 w-14" />
					</div>
				))}
			</div>
			<div className="card p-4">
				<Skeleton className="h-28 w-full" />
			</div>
		</div>
	);
}

export function OverviewPage() {
	const [range, setRange] = useState<OverviewRange>('1h');
	const overview = useOverview(range);
	const data = overview.data;
	const rangeLabel = range === '1h' ? 'Last hour' : 'Last 24 hours';

	return (
		<>
			<PageHeader
				title="Overview"
				description="Queue depth, workflow health and what needs attention."
				actions={
					<>
						<LiveIndicator updatedAt={overview.dataUpdatedAt} error={overview.isError && Boolean(data)} />
						<RangeToggle value={range} onChange={setRange} />
					</>
				}
			/>
			{overview.isPending ? <OverviewSkeleton /> : null}
			{overview.isError && !data ? <ErrorState error={overview.error} onRetry={() => overview.refetch()} className="card" /> : null}
			{data ? (
				<div className="space-y-5 md:space-y-6">
					<Attention data={data} />
					<section aria-label="Queue jobs">
						<h2 className="mb-2.5 flex items-center gap-2 text-[0.8125rem] font-semibold text-muted">
							<Layers aria-hidden className="size-3.5" />
							Queue jobs
							<span className="tabular font-normal text-subtle">
								· {plural(data.queue.summary.queues, 'queue', 'queues')} · {plural(data.queue.summary.advisory_locks, 'lock', 'locks')}
							</span>
						</h2>
						<div className="grid grid-cols-2 gap-3 md:grid-cols-4">
							<StatTile
								to="/queues?status=pending"
								label="Due now"
								value={data.queue.due_jobs}
								detail={`${formatNumber(Math.max(0, data.queue.summary.pending_jobs - data.queue.due_jobs))} scheduled later`}
							/>
							<StatTile to="/queues?status=processing" label="Processing" value={data.queue.summary.processing_jobs} />
							<StatTile
								to="/queues?status=failed"
								label="Failed"
								value={data.queue.summary.failed_jobs}
								alert={data.queue.summary.failed_jobs > 0}
							/>
							<StatTile to="/queues?status=done" label="Done" value={data.queue.summary.done_jobs} />
						</div>
					</section>
					{data.workflow ? (
						<section aria-label="Workflow runs">
							<h2 className="mb-2.5 flex items-center gap-2 text-[0.8125rem] font-semibold text-muted">
								<Workflow aria-hidden className="size-3.5" />
								Workflow runs
								<span className="tabular font-normal text-subtle">· {formatNumber(data.workflow.counts.total)} total</span>
							</h2>
							<div className="grid grid-cols-2 gap-3 md:grid-cols-4">
								<StatTile
									to="/workflows?status=running&children=1"
									label="Running"
									value={data.workflow.counts.running + data.workflow.counts.pending}
									detail={data.workflow.counts.pending ? `${formatNumber(data.workflow.counts.pending)} pending` : undefined}
								/>
								<StatTile to="/workflows?status=waiting&children=1" label="Waiting" value={data.workflow.counts.waiting} />
								<StatTile
									to="/workflows?status=failed&children=1"
									label="Failed"
									value={data.workflow.counts.failed}
									alert={data.workflow.counts.failed > 0}
								/>
								<StatTile
									to="/workflows?status=succeeded&children=1"
									label="Succeeded"
									value={data.workflow.counts.succeeded}
									detail={data.workflow.counts.cancelled ? `${formatNumber(data.workflow.counts.cancelled)} cancelled` : undefined}
								/>
							</div>
						</section>
					) : null}

					<Section title="Throughput" description={`Completions per ${bucketLabel(data.queue.throughput.bucket_seconds)}`}>
						<div className={cn('grid gap-6', data.workflow && 'lg:grid-cols-2 lg:gap-8')}>
							<ThroughputChart title="Jobs" buckets={data.queue.throughput.buckets} series={jobSeries} rangeLabel={rangeLabel} />
							{data.workflow ? (
								<ThroughputChart title="Runs" buckets={data.workflow.throughput.buckets} series={runSeries} rangeLabel={rangeLabel} />
							) : null}
						</div>
					</Section>

					<div className="grid gap-5 lg:grid-cols-2 lg:gap-6">
						<div className="flex min-w-0 flex-col gap-5 lg:gap-6">
							{data.workflow ? (
								<Section
									id="waiting"
									title="Waiting runs"
									description="What each run is waiting for"
									flush
									action={<SectionLink to="/workflows?status=waiting&children=1">All</SectionLink>}
								>
									<WaitingList runs={data.workflow.waiting} total={data.workflow.counts.waiting} />
								</Section>
							) : null}
							<Section id="failures" title="Recent failures" flush>
								<FailureList runs={data.workflow?.failed ?? []} jobs={data.queue.failed_jobs} />
							</Section>
						</div>
						<Section
							id="activity"
							title="Recent activity"
							description="Latest job and run updates"
							flush
							action={<Activity aria-hidden className="size-4 text-subtle" />}
						>
							<ActivityFeed data={data} />
						</Section>
					</div>
				</div>
			) : null}
		</>
	);
}
