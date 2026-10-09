import { ChevronRight, CornerDownRight, Workflow } from 'lucide-react';
import { Link } from 'react-router';
import type { RunSummary } from '../api/types';
import { useNow } from '../lib/clock';
import { cn } from '../lib/cn';
import { describeStep } from '../lib/describe';
import { firstLine, formatRelative, plural, shortId } from '../lib/format';
import { toneText } from '../lib/status';
import { StatusBadge } from './StatusBadge';
import { Elapsed } from './Time';

export function RunIdentity({ run, className }: { run: Pick<RunSummary, 'id' | 'workflow' | 'version' | 'key'>; className?: string }) {
	return (
		<span className={cn('flex min-w-0 flex-col', className)}>
			<span className="flex min-w-0 items-baseline gap-1.5">
				<span className="truncate font-medium text-fg">{run.workflow}</span>
				{run.version > 0 ? <span className="tabular shrink-0 text-xs text-subtle">v{run.version}</span> : null}
			</span>
			<span className="truncate font-mono text-xs text-muted" title={run.key ?? run.id}>
				{run.key ?? shortId(run.id)}
			</span>
		</span>
	);
}

export function RunProgress({ run, className }: { run: RunSummary; className?: string }) {
	const now = useNow();
	if (run.current_step && run.status !== 'succeeded' && run.status !== 'cancelled') {
		const description = describeStep(run.current_step, now);
		return (
			<span className={cn('flex min-w-0 flex-col', className)}>
				<span className={cn('truncate', toneText[description.tone])}>{description.text}</span>
				{description.detail ? <span className="tabular truncate text-xs text-muted">{description.detail}</span> : null}
			</span>
		);
	}
	if (run.error) {
		return (
			<span
				className={cn('line-clamp-2 [overflow-wrap:anywhere]', run.status === 'cancelled' ? 'text-muted' : 'text-bad', className)}
				title={run.error}
			>
				{firstLine(run.error, 200)}
			</span>
		);
	}
	return (
		<span className={cn('truncate text-muted', className)}>
			{run.step_count > 0 ? plural(run.step_count, 'checkpoint', 'checkpoints') : 'No checkpoints yet'}
		</span>
	);
}

export function RunRow({ run, showParent = false }: { run: RunSummary; showParent?: boolean }) {
	const now = useNow();
	return (
		<li className="@container">
			<Link
				to={`/workflows/${encodeURIComponent(run.id)}`}
				data-row
				className="group relative flex flex-col gap-2 px-4 py-3 text-[0.8125rem] transition-colors outline-offset-[-2px] hover:bg-surface-2 focus-visible:bg-surface-2 @3xl:grid @3xl:grid-cols-[6.5rem_minmax(0,1.3fr)_minmax(0,1.6fr)_5.5rem_5rem_1rem] @3xl:items-center @3xl:gap-4 @3xl:py-2.5"
			>
				<span className="flex items-center justify-between gap-3 @3xl:contents">
					<StatusBadge status={run.status} size="sm" className="@3xl:justify-self-start" />
					<span className="tabular text-xs text-muted @3xl:order-5 @3xl:text-right">{formatRelative(run.created_at, now)}</span>
				</span>
				<RunIdentity run={run} className="@3xl:order-2" />
				<RunProgress run={run} className="@3xl:order-3" />
				<span className="flex items-center gap-3 text-xs text-muted @3xl:order-4 @3xl:flex-col @3xl:items-end @3xl:gap-0">
					{run.child_count > 0 ? (
						<span className="tabular inline-flex items-center gap-1" title={plural(run.child_count, 'child run', 'child runs')}>
							<Workflow aria-hidden className="size-3" />
							{plural(run.child_count, 'child', 'children')}
						</span>
					) : null}
					{showParent && run.parent_run_id ? (
						<span className="inline-flex items-center gap-1">
							<CornerDownRight aria-hidden className="size-3" />
							child run
						</span>
					) : null}
					<span className="@3xl:hidden">
						{run.completed_at ? 'took ' : 'for '}
						<Elapsed from={run.started_at ?? run.created_at} to={run.completed_at} />
					</span>
				</span>
				<ChevronRight aria-hidden className="hidden size-4 text-subtle transition-transform motion-safe:group-hover:translate-x-0.5 @3xl:order-6 @3xl:block" />
			</Link>
		</li>
	);
}
