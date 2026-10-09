import { ChevronRight } from 'lucide-react';
import { Link } from 'react-router';
import type { QueueJob } from '../api/types';
import { useNow } from '../lib/clock';
import { firstLine, formatRelative, isParked, jobLabel } from '../lib/format';
import { MiddleTruncate } from './MiddleTruncate';
import { StatusBadge } from './StatusBadge';

function jobTiming(job: QueueJob, now: number): string {
	if (job.status === 'processing') return `claimed ${formatRelative(job.claimed_at, now)}`;
	if (job.status === 'done') return `done ${formatRelative(job.done_at ?? job.updated_at, now)}`;
	if (job.status === 'failed') return `failed ${formatRelative(job.updated_at, now)}`;
	if (isParked(job.available_at)) return 'parked until woken';
	const available = Date.parse(job.available_at);
	if (!Number.isNaN(available) && available > now) return `runs ${formatRelative(job.available_at, now)}`;
	return `queued ${formatRelative(job.created_at, now)}`;
}

export function JobRow({ job, href }: { job: QueueJob; href: string }) {
	const now = useNow();
	return (
		<li className="@container">
			<Link
				to={href}
				data-row
				className="group flex flex-col gap-1.5 px-4 py-3 text-[0.8125rem] transition-colors outline-offset-[-2px] hover:bg-surface-2 focus-visible:bg-surface-2 @3xl:grid @3xl:grid-cols-[5.5rem_6.5rem_minmax(0,1fr)_minmax(0,1.6fr)_7.5rem_1rem] @3xl:items-center @3xl:gap-4 @3xl:py-2.5"
			>
				<span className="flex items-center gap-2 @3xl:contents">
					<span className="tabular font-mono text-xs text-muted">{jobLabel(job.id)}</span>
					<StatusBadge status={job.status} size="sm" className="@3xl:justify-self-start" />
					<span className="tabular ml-auto text-xs text-muted @3xl:order-5 @3xl:ml-0 @3xl:text-right">
						{jobTiming(job, now)}
					</span>
				</span>
				<span className="flex min-w-0 items-baseline gap-2 @3xl:order-3">
					<MiddleTruncate text={job.queue_name} tail={10} className="font-mono text-[0.8125rem] text-fg" />
					<span className="tabular shrink-0 text-xs text-subtle" title="Attempts used / allowed">
						{job.attempts}/{job.max_attempts}
					</span>
				</span>
				<span className="min-w-0 @3xl:order-4">
					{job.last_error ? (
						<span className="line-clamp-2 text-bad [overflow-wrap:anywhere]" title={job.last_error}>
							{firstLine(job.last_error, 200)}
						</span>
					) : (
						<span className="block truncate font-mono text-xs text-muted" title={job.payload_preview}>
							{job.payload_preview || 'Empty payload'}
						</span>
					)}
				</span>
				<ChevronRight aria-hidden className="hidden size-4 text-subtle transition-transform motion-safe:group-hover:translate-x-0.5 @3xl:order-6 @3xl:block" />
			</Link>
		</li>
	);
}
