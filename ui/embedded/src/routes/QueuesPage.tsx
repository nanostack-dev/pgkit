import { Inbox, Layers, Plus } from 'lucide-react';
import { useState } from 'react';
import { useSearchParams } from 'react-router';
import { useConfig, useJobs, useQueues } from '../api/queries';
import { jobStatuses, type JobStatus, type QueueStats } from '../api/types';
import { Button } from '../components/Button';
import { Pagination, SearchInput, SegmentedFilter, SelectField } from '../components/Filters';
import { JobRow } from '../components/JobRow';
import { jobHref } from '../lib/links';
import { LiveIndicator, PageHeader, Section } from '../components/Layout';
import { EmptyState, ErrorState, ListSkeleton, Skeleton } from '../components/States';
import { MiddleTruncate } from '../components/MiddleTruncate';
import { useNow } from '../lib/clock';
import { cn } from '../lib/cn';
import { useIsDesktop } from '../lib/media';
import { formatCompact, formatDuration, formatNumber, parseTime, plural } from '../lib/format';
import { statusLabel } from '../lib/status';
import { EnqueueDialog } from './queues/EnqueueDialog';
import { JobSheet } from './queues/JobSheet';


function readStatus(value: string | null): JobStatus | undefined {
	return jobStatuses.includes(value as JobStatus) ? (value as JobStatus) : undefined;
}

function QueueBar({ stats }: { stats: QueueStats }) {
	const total = Math.max(1, stats.total);
	const parts = [
		{ key: 'pending', value: stats.pending, className: 'bg-idle/60' },
		{ key: 'processing', value: stats.processing, className: 'bg-run' },
		{ key: 'done', value: stats.done, className: 'bg-ok/70' },
		{ key: 'failed', value: stats.failed, className: 'bg-bad' },
	];
	return (
		<div aria-hidden className="flex h-1.5 w-full overflow-hidden rounded-full bg-surface-3">
			{parts.map((part) =>
				part.value > 0 ? <div key={part.key} className={part.className} style={{ width: `${(part.value / total) * 100}%` }} /> : null,
			)}
		</div>
	);
}

function QueueCard({ stats, selected, onSelect }: { stats: QueueStats; selected: boolean; onSelect: () => void }) {
	const now = useNow();
	const oldestDue = parseTime(stats.oldest_due_at);
	const lag = oldestDue === null ? 0 : now - oldestDue;
	return (
		<li className="min-w-0">
			<button
				type="button"
				onClick={onSelect}
				aria-pressed={selected}
				className={cn(
					'press flex w-full min-w-0 flex-col gap-2.5 rounded-xl border bg-surface p-3.5 text-left transition-[background-color,border-color,transform] hover:bg-surface-2',
					selected ? 'border-fg' : 'border-line',
				)}
			>
				<span className="flex w-full min-w-0 items-baseline justify-between gap-3">
					<MiddleTruncate text={stats.queue_name} tail={10} className="font-mono text-[0.8125rem] font-medium" />
					<span className="tabular shrink-0 text-xs text-muted">{formatCompact(stats.total)}</span>
				</span>
				<QueueBar stats={stats} />
				<span className="tabular flex flex-wrap gap-x-3 gap-y-0.5 text-xs text-muted">
					<span>{formatNumber(stats.pending)} pending</span>
					{stats.processing ? <span className="text-run">{formatNumber(stats.processing)} processing</span> : null}
					{stats.failed ? <span className="text-bad">{formatNumber(stats.failed)} failed</span> : null}
					<span>{formatNumber(stats.done)} done</span>
				</span>
				{lag > 60_000 ? (
					<span className="text-xs text-warn">
						{plural(stats.due, 'due job', 'due jobs')} · oldest waiting {formatDuration(lag)}
					</span>
				) : null}
			</button>
		</li>
	);
}

export function QueuesPage() {
	const [params, setParams] = useSearchParams();
	const config = useConfig();
	const queues = useQueues();
	const [showAllQueues, setShowAllQueues] = useState(false);
	const collapsedQueueCount = useIsDesktop() ? 6 : 3;
	const [enqueueOpen, setEnqueueOpen] = useState(false);
	const pageSize = config.data?.page_size ?? 20;
	const mutationsEnabled = config.data?.mutations_enabled ?? false;

	const queue = params.get('queue') ?? undefined;
	const status = readStatus(params.get('status'));
	const search = params.get('search') ?? '';
	const offset = Math.max(0, Number(params.get('offset') ?? 0) || 0);
	const jobParam = params.get('job');
	const jobId = jobParam && /^\d+$/.test(jobParam) ? Number(jobParam) : null;

	const filters = { queue, status, search: search || undefined, limit: pageSize, offset };
	const jobs = useJobs(filters);

	function update(changes: Record<string, string | undefined>, resetOffset = true) {
		setParams(
			(current) => {
				const next = new URLSearchParams(current);
				for (const [key, value] of Object.entries(changes)) {
					if (value === undefined || value === '') next.delete(key);
					else next.set(key, value);
				}
				if (resetOffset) next.delete('offset');
				return next;
			},
			{ replace: true },
		);
	}

	const queueItems = queues.data?.items ?? [];
	const selectedStats = queue ? queueItems.find((item) => item.queue_name === queue) : undefined;
	const countSource = selectedStats ? [selectedStats] : queueItems;
	const counts: Record<JobStatus, number> = {
		pending: countSource.reduce((sum, item) => sum + item.pending, 0),
		processing: countSource.reduce((sum, item) => sum + item.processing, 0),
		done: countSource.reduce((sum, item) => sum + item.done, 0),
		failed: countSource.reduce((sum, item) => sum + item.failed, 0),
	};

	const visibleQueues = showAllQueues ? queueItems : queueItems.slice(0, collapsedQueueCount);
	const hasFilters = Boolean(queue || status || search);
	const closeJobHref = () => {
		const next = new URLSearchParams(params);
		next.delete('job');
		return next;
	};

	return (
		<>
			<PageHeader
				title="Queues"
				description="Durable jobs by queue. Filter, inspect payloads and replay failures."
				actions={
					<>
						<LiveIndicator updatedAt={jobs.dataUpdatedAt} error={jobs.isError && Boolean(jobs.data)} />
						{mutationsEnabled ? (
							<Button variant="primary" size="sm" icon={Plus} onClick={() => setEnqueueOpen(true)}>
								Enqueue job
							</Button>
						) : null}
					</>
				}
			/>

			<section aria-labelledby="queue-list-title" className="mb-6">
				<div className="mb-2.5 flex items-center justify-between gap-3">
					<h2 id="queue-list-title" className="text-[0.8125rem] font-semibold text-muted">
						{queues.data ? plural(queueItems.length, 'queue', 'queues') : 'Queues'}
					</h2>
					{queue ? (
						<button type="button" onClick={() => update({ queue: undefined })} className="min-h-8 text-xs font-medium text-muted hover:text-fg">
							Show all queues
						</button>
					) : null}
				</div>
				{queues.isPending ? (
					<div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
						{Array.from({ length: 3 }, (_, index) => (
							<div key={index} className="card space-y-3 p-3.5">
								<Skeleton className="h-3.5 w-1/2" />
								<Skeleton className="h-1.5 w-full" />
								<Skeleton className="h-3 w-2/3" />
							</div>
						))}
					</div>
				) : queues.isError && !queues.data ? (
					<ErrorState error={queues.error} onRetry={() => queues.refetch()} className="card" />
				) : queueItems.length === 0 ? (
					<div className="card">
						<EmptyState icon={Layers} title="No queues yet">
							A queue appears once a job is enqueued on it.
						</EmptyState>
					</div>
				) : (
					<>
						<ul className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
							{visibleQueues.map((item) => (
								<QueueCard
									key={item.queue_name}
									stats={item}
									selected={item.queue_name === queue}
									onSelect={() => update({ queue: item.queue_name === queue ? undefined : item.queue_name })}
								/>
							))}
						</ul>
						{queueItems.length > collapsedQueueCount ? (
							<button
								type="button"
								onClick={() => setShowAllQueues((current) => !current)}
								className="mt-2 min-h-10 text-[0.8125rem] font-medium text-muted hover:text-fg"
							>
								{showAllQueues ? 'Show fewer queues' : `Show all ${formatNumber(queueItems.length)} queues`}
							</button>
						) : null}
					</>
				)}
			</section>

			<Section title="Jobs" description={queue ? `In ${queue}` : 'Across all queues'} flush>
				<div className="flex flex-col gap-2.5 border-b border-line p-3 md:p-4">
					<div className="flex flex-col gap-2.5 lg:flex-row">
						<SearchInput
							value={search}
							onChange={(value) => update({ search: value })}
							label="Search jobs"
							placeholder="Search queue, payload or error"
							className="lg:flex-1"
						/>
						<SelectField
							label="Queue"
							value={queue}
							placeholder="All queues"
							options={queueItems.map((item) => ({ value: item.queue_name, label: item.queue_name, hint: formatCompact(item.total) }))}
							onChange={(value) => update({ queue: value })}
							className="lg:w-64"
						/>
					</div>
					<SegmentedFilter
						label="Job status"
						value={status}
						options={jobStatuses.map((value) => ({ value, label: statusLabel(value), count: queues.data ? counts[value] : undefined }))}
						onChange={(value) => update({ status: value })}
					/>
				</div>
				{jobs.isPending ? (
					<ListSkeleton label="Loading jobs" />
				) : jobs.isError && !jobs.data ? (
					<ErrorState error={jobs.error} onRetry={() => jobs.refetch()} />
				) : jobs.data && jobs.data.items.length === 0 ? (
					<EmptyState
						icon={Inbox}
						title={hasFilters ? 'No jobs match these filters' : 'No jobs yet'}
						action={
							hasFilters ? (
								<Button size="sm" onClick={() => update({ queue: undefined, status: undefined, search: undefined })}>
									Clear filters
								</Button>
							) : undefined
						}
					>
						{hasFilters ? 'Try another status or a broader search.' : 'Jobs appear here as soon as they are enqueued.'}
					</EmptyState>
				) : jobs.data ? (
					<ul className={cn('divide-y divide-line transition-opacity', jobs.isPlaceholderData && 'opacity-60')} aria-busy={jobs.isPlaceholderData}>
						{jobs.data.items.map((job) => (
							<JobRow key={job.id} job={job} href={jobHref(job.id, params)} />
						))}
					</ul>
				) : null}
				{jobs.data ? (
					<div className="border-t border-line px-3 pb-3 md:px-4">
						<Pagination
							noun="Jobs"
							total={jobs.data.total}
							limit={pageSize}
							offset={offset}
							onOffsetChange={(next) => update({ offset: next ? String(next) : undefined }, false)}
						/>
					</div>
				) : null}
			</Section>

			<JobSheet
				jobId={jobId}
				mutationsEnabled={mutationsEnabled}
				onClose={() => setParams(closeJobHref(), { replace: false })}
			/>
			<EnqueueDialog
				open={enqueueOpen}
				onOpenChange={setEnqueueOpen}
				queues={queueItems.map((item) => item.queue_name)}
				defaultQueue={queue}
				onEnqueued={(id) => update({ job: String(id) }, false)}
			/>
		</>
	);
}
