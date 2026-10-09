import { ArrowUpRight, RotateCcw, Trash2 } from 'lucide-react';
import { useState } from 'react';
import { Link } from 'react-router';
import { toast } from 'sonner';
import { ApiError, errorMessage } from '../../api/client';
import { useDeleteJob, useJob, useReplayJob } from '../../api/queries';
import type { QueueJobDetail } from '../../api/types';
import { Button } from '../../components/Button';
import { CopyButton } from '../../components/Copy';
import { ConfirmDialog } from '../../components/Dialogs';
import { JsonView } from '../../components/JsonView';
import { MiddleTruncate } from '../../components/MiddleTruncate';
import { Fact } from '../../components/Layout';
import { Sheet } from '../../components/Sheet';
import { StatusBadge } from '../../components/StatusBadge';
import { EmptyState, ErrorState, Skeleton } from '../../components/States';
import { Countdown, RelativeTime } from '../../components/Time';
import { useNow } from '../../lib/clock';
import { cn } from '../../lib/cn';
import { formatDateTime, formatNumber, isParked, parseTime, shortId, jobLabel } from '../../lib/format';
import { jobReplayable } from '../../lib/status';

function Attempts({ used, max }: { used: number; max: number }) {
	const dots = Math.min(max, 12);
	return (
		<span className="inline-flex items-center gap-2">
			{max <= 12 ? (
				<span aria-hidden className="flex gap-1">
					{Array.from({ length: dots }, (_, index) => (
						<span key={index} className={cn('size-1.5 rounded-full', index < used ? 'bg-fg' : 'bg-line-strong')} />
					))}
				</span>
			) : null}
			<span className="tabular">
				{formatNumber(used)} of {formatNumber(max)}
			</span>
		</span>
	);
}

function JobDetail({ job }: { job: QueueJobDetail }) {
	const now = useNow();
	const available = parseTime(job.available_at);
	const parked = job.status === 'pending' && isParked(job.available_at);
	const scheduled = job.status === 'pending' && !parked && available !== null && available > now;
	return (
		<div className="space-y-5">
			<div className="flex flex-wrap items-center gap-2">
				<StatusBadge status={job.status} />
				{scheduled ? (
					<span className="text-[0.8125rem] text-muted">
						runs in <Countdown value={job.available_at} className="text-fg" />
					</span>
				) : null}
			</div>

			<dl className="grid grid-cols-2 gap-x-4 gap-y-3.5">
				<Fact label="Attempts">
					<Attempts used={job.attempts} max={job.max_attempts} />
				</Fact>
				<Fact label="Created">
					<RelativeTime value={job.created_at} />
				</Fact>
				<Fact label={scheduled ? 'Available at' : 'Available since'}>
					{parked ? (
						<span>Parked until woken</span>
					) : (
						<span title={formatDateTime(job.available_at)}>{formatDateTime(job.available_at)}</span>
					)}
				</Fact>
				<Fact label="Updated">
					<RelativeTime value={job.updated_at} />
				</Fact>
				{job.claimed_by ? (
					<Fact label="Claimed by" className="col-span-2">
						<code className="block font-mono text-[0.8125rem] [overflow-wrap:anywhere]">{job.claimed_by}</code>
						{job.claimed_at ? (
							<span className="text-xs text-muted">
								claimed <RelativeTime value={job.claimed_at} />
							</span>
						) : null}
					</Fact>
				) : null}
				{job.done_at ? (
					<Fact label="Done">
						<RelativeTime value={job.done_at} />
					</Fact>
				) : null}
			</dl>

			{job.run_id ? (
				<Link
					to={`/workflows/${encodeURIComponent(job.run_id)}`}
					className="press flex min-h-11 items-center gap-3 rounded-xl border border-line bg-surface-2 px-3.5 py-2.5 text-[0.8125rem] transition-[background-color,transform] hover:bg-surface-3"
				>
					<span className="min-w-0 flex-1">
						<span className="block font-medium">Workflow run job</span>
						<span className="block truncate font-mono text-xs text-muted">{shortId(job.run_id)}</span>
					</span>
					<ArrowUpRight aria-hidden className="size-4 text-muted" />
				</Link>
			) : null}

			{job.last_error ? (
				<div className="overflow-hidden rounded-lg border border-bad/25 bg-bad-soft">
					<div className="flex items-center justify-between gap-2 border-b border-bad/20 py-1 pr-1.5 pl-3">
						<span className="text-xs font-medium text-bad">Last error</span>
						<CopyButton value={job.last_error} label="Copy error" />
					</div>
					<pre className="max-h-64 overflow-auto overscroll-contain px-3 py-2.5 font-mono text-[0.75rem] leading-relaxed whitespace-pre-wrap text-fg [overflow-wrap:anywhere]">
						{job.last_error}
					</pre>
				</div>
			) : null}

			<JsonView
				label="Payload"
				value={job.payload}
				encoding={job.payload_encoding}
				bytes={job.payload_bytes}
				truncated={job.payload_truncated}
				emptyLabel="Empty payload"
			/>
		</div>
	);
}

function JobActions({ job, mutationsEnabled, onDeleted }: { job: QueueJobDetail; mutationsEnabled: boolean; onDeleted: () => void }) {
	const [confirm, setConfirm] = useState<'replay' | 'delete' | null>(null);
	const replay = useReplayJob();
	const remove = useDeleteJob();
	const canReplay = jobReplayable(job.status);
	const canDelete = job.status !== 'processing';

	if (!mutationsEnabled) {
		return <p className="text-xs text-muted">Read-only server: replay and delete are disabled.</p>;
	}

	return (
		<>
			<div className="flex gap-2">
				<Button
					variant="primary"
					icon={RotateCcw}
					className="flex-1 md:flex-none"
					disabled={!canReplay}
					title={canReplay ? undefined : 'Only done or failed jobs can be replayed'}
					onClick={() => setConfirm('replay')}
				>
					Replay
				</Button>
				<Button
					variant="danger-outline"
					icon={Trash2}
					className="flex-1 md:flex-none"
					disabled={!canDelete}
					title={canDelete ? undefined : 'A processing job cannot be deleted'}
					onClick={() => setConfirm('delete')}
				>
					Delete
				</Button>
			</div>
			{!canReplay && job.status !== 'processing' ? (
				<p className="mt-2 text-xs text-muted">Pending jobs run on their own; replay applies to done or failed jobs.</p>
			) : null}
			{job.status === 'processing' ? <p className="mt-2 text-xs text-muted">A worker holds this job; wait for it to finish.</p> : null}
			<ConfirmDialog
				open={confirm === 'replay'}
				onOpenChange={(open) => setConfirm(open ? 'replay' : null)}
				title={`Replay job ${jobLabel(job.id)}?`}
				description="The job returns to pending with its attempts reset, and a worker runs it again. Its handler's side effects must be safe to repeat."
				confirmLabel="Replay job"
				pending={replay.isPending}
				onConfirm={() =>
					replay.mutate(job.id, {
						onSuccess: () => {
							setConfirm(null);
							toast.success(`Job ${jobLabel(job.id)} is pending again.`);
						},
						onError: (error) => toast.error(errorMessage(error)),
					})
				}
			/>
			<ConfirmDialog
				open={confirm === 'delete'}
				onOpenChange={(open) => setConfirm(open ? 'delete' : null)}
				title={`Delete job ${jobLabel(job.id)}?`}
				description={
					job.run_id
						? 'This job drives a workflow run. Deleting it leaves the run without a job until the run is retried. The job and its payload are removed permanently.'
						: 'The job and its payload are removed permanently. This cannot be undone.'
				}
				confirmLabel="Delete job"
				tone="danger"
				pending={remove.isPending}
				onConfirm={() =>
					remove.mutate(job.id, {
						onSuccess: () => {
							setConfirm(null);
							toast.success(`Job ${jobLabel(job.id)} deleted.`);
							onDeleted();
						},
						onError: (error) => toast.error(errorMessage(error)),
					})
				}
			/>
		</>
	);
}

export function JobSheet({
	jobId,
	mutationsEnabled,
	onClose,
}: {
	jobId: number | null;
	mutationsEnabled: boolean;
	onClose: () => void;
}) {
	const [shownId, setShownId] = useState(jobId);
	if (jobId !== null && jobId !== shownId) setShownId(jobId);
	const job = useJob(shownId);
	const notFound = job.error instanceof ApiError && job.error.status === 404;

	return (
		<Sheet
			open={jobId !== null}
			onOpenChange={(open) => {
				if (!open) onClose();
			}}
			title={shownId !== null ? `Job ${jobLabel(shownId)}` : 'Job'}
			subtitle={job.data ? <MiddleTruncate text={job.data.queue_name} tail={10} className="font-mono" /> : undefined}
			footer={job.data ? <JobActions job={job.data} mutationsEnabled={mutationsEnabled} onDeleted={onClose} /> : undefined}
		>
			{job.isPending ? (
				<div role="status" aria-label="Loading job" className="space-y-4">
					<Skeleton className="h-6 w-24 rounded-full" />
					<div className="grid grid-cols-2 gap-4">
						<Skeleton className="h-9" />
						<Skeleton className="h-9" />
						<Skeleton className="h-9" />
						<Skeleton className="h-9" />
					</div>
					<Skeleton className="h-40" />
				</div>
			) : notFound ? (
				<EmptyState icon={Trash2} title="This job no longer exists">
					It was deleted or purged after completion.
				</EmptyState>
			) : job.isError ? (
				<ErrorState error={job.error} onRetry={() => job.refetch()} />
			) : job.data ? (
				<JobDetail job={job.data} />
			) : null}
		</Sheet>
	);
}
