import { Collapsible } from '@base-ui/react/collapsible';
import { ArrowUpRight, ChevronRight, ListTree } from 'lucide-react';
import { useState } from 'react';
import { Link } from 'react-router';
import type { RunSummary, WorkflowStep } from '../../api/types';
import { CopyButton } from '../../components/Copy';
import { JsonView } from '../../components/JsonView';
import { EmptyState } from '../../components/States';
import { StatusBadge, StatusDot } from '../../components/StatusBadge';
import { Countdown, Elapsed } from '../../components/Time';
import { useNow } from '../../lib/clock';
import { cn } from '../../lib/cn';
import { describeStep } from '../../lib/describe';
import { formatClock, formatDateTime, formatNumber, plural, shortId, splitStepName } from '../../lib/format';
import { kindMeta, statusTone, toneSoft, toneText } from '../../lib/status';

const errorPreviewLines = 4;
const leadingCheckpoints = 20;
const trailingCheckpoints = 60;

function ErrorBlock({ error }: { error: string }) {
	const [expanded, setExpanded] = useState(false);
	const lines = error.split('\n');
	const long = lines.length > errorPreviewLines + 1 || error.length > 600;
	const visible = long && !expanded ? `${lines.slice(0, errorPreviewLines).join('\n').slice(0, 600)}…` : error;
	return (
		<div className="mt-2 overflow-hidden rounded-lg border border-bad/25 bg-bad-soft">
			<div className="flex items-start gap-2 py-2 pr-1.5 pl-3">
				<pre className="min-w-0 flex-1 font-mono text-[0.75rem] leading-relaxed whitespace-pre-wrap text-fg [overflow-wrap:anywhere]">
					{visible}
				</pre>
				<CopyButton value={error} label="Copy error" className="-mt-0.5" />
			</div>
			{long ? (
				<button
					type="button"
					aria-expanded={expanded}
					onClick={() => setExpanded((current) => !current)}
					className="flex min-h-9 w-full items-center justify-center border-t border-bad/20 text-xs font-medium text-bad hover:bg-bad/5 md:min-h-7"
				>
					{expanded ? 'Show less' : 'Show full error'}
				</button>
			) : null}
		</div>
	);
}

function ChildCard({ childId, child }: { childId: string; child: RunSummary | undefined }) {
	return (
		<Link
			to={`/workflows/${encodeURIComponent(childId)}`}
			className="press mt-2 flex min-h-11 items-center gap-3 rounded-lg border border-line bg-surface px-3 py-2 text-[0.8125rem] transition-[background-color,transform] hover:bg-surface-2"
		>
			{child ? <StatusDot status={child.status} /> : null}
			<span className="flex min-w-0 flex-1 flex-col">
				<span className="truncate font-medium">{child ? child.workflow : 'Child run'}</span>
				<span className="truncate font-mono text-xs text-muted">{child?.key ?? shortId(childId)}</span>
			</span>
			{child ? <StatusBadge status={child.status} size="sm" /> : null}
			<ArrowUpRight aria-hidden className="size-4 shrink-0 text-subtle" />
		</Link>
	);
}

function OutputToggle({ step }: { step: WorkflowStep }) {
	const label = step.kind === 'signal' ? 'Signal payload' : step.kind === 'child' ? 'Child result' : 'Output';
	return (
		<Collapsible.Root className="mt-1.5">
			<Collapsible.Trigger className="group inline-flex min-h-9 items-center gap-1 rounded-md pr-2 text-xs font-medium text-muted hover:text-fg md:min-h-7">
				<ChevronRight aria-hidden className="size-3.5 transition-transform duration-150 group-data-[panel-open]:rotate-90" />
				{label}
			</Collapsible.Trigger>
			<Collapsible.Panel className="collapsible-panel">
				<JsonView value={step.output} label={label} className="mt-1" />
			</Collapsible.Panel>
		</Collapsible.Root>
	);
}

function TimelineItem({ step, child, last }: { step: WorkflowStep; child: RunSummary | undefined; last: boolean }) {
	const now = useNow();
	const meta = kindMeta[step.kind];
	const Icon = meta.icon;
	const tone = statusTone(step.status);
	const { base, repeat } = splitStepName(step.name);
	const description = step.status === 'succeeded' ? null : describeStep(step, now);
	const finished = step.status === 'succeeded' || step.status === 'failed' || step.status === 'timed_out';

	return (
		<li className="relative grid grid-cols-[2rem_minmax(0,1fr)] gap-x-3">
			{last ? null : <span aria-hidden className="absolute top-9 bottom-0 left-[0.9375rem] w-px bg-line" />}
			<span aria-hidden className={cn('relative mt-0.5 flex size-8 items-center justify-center rounded-full', toneSoft[tone])}>
				<Icon className="size-4" strokeWidth={2} />
			</span>
			<div className="min-w-0 pb-5">
				<div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1">
					<h3 className="min-w-0 font-mono text-[0.8125rem] font-medium [overflow-wrap:anywhere]">
						{base}
						{repeat ? <span className="ml-1 rounded bg-surface-3 px-1 text-[0.6875rem] text-muted">#{repeat}</span> : null}
					</h3>
					<StatusBadge status={step.status} size="sm" />
				</div>
				<p className="tabular mt-1 flex flex-wrap gap-x-2 gap-y-0.5 text-xs text-muted">
					<span>{meta.label}</span>
					{step.kind === 'signal' && step.signal && step.signal !== base ? (
						<span className="font-mono">· {step.signal}</span>
					) : null}
					{step.attempts > 1 || step.kind === 'step' ? <span>· {plural(step.attempts, 'attempt', 'attempts')}</span> : null}
					<span title={formatDateTime(step.created_at)}>· {formatClock(step.created_at, true)}</span>
					<span>
						· {finished ? 'took ' : 'for '}
						<Elapsed from={step.created_at} to={finished ? (step.completed_at ?? step.updated_at) : null} />
					</span>
				</p>
				{description ? (
					<p className={cn('mt-1.5 text-[0.8125rem]', toneText[description.tone])}>
						{description.text}
						{step.wake_at && (step.status === 'waiting' || step.status === 'retrying') ? (
							<span className="text-muted">
								{' · '}
								<Countdown
									value={step.wake_at}
									prefix={step.kind === 'signal' ? 'times out in' : step.status === 'retrying' ? 'next attempt in' : 'wakes in'}
									overdue={step.kind === 'signal' ? 'timeout reached' : step.status === 'retrying' ? 'retrying now' : 'waking up now'}
								/>
							</span>
						) : step.kind === 'signal' && step.status === 'waiting' ? (
							<span className="text-muted"> · no timeout</span>
						) : null}
					</p>
				) : null}
				{step.kind === 'child' && step.child_run_id ? <ChildCard childId={step.child_run_id} child={child} /> : null}
				{step.error ? <ErrorBlock error={step.error} /> : null}
				{step.output ? <OutputToggle step={step} /> : null}
			</div>
		</li>
	);
}

export function Timeline({ steps, children, runFinished }: { steps: WorkflowStep[]; children: RunSummary[]; runFinished: boolean }) {
	const [issuesOnly, setIssuesOnly] = useState(false);
	const [showAll, setShowAll] = useState(false);
	const childById = new Map(children.map((child) => [child.id, child]));
	const issues = steps.filter((step) => step.status !== 'succeeded');
	const visible = issuesOnly ? issues : steps;
	const hiddenCount = showAll ? 0 : Math.max(0, visible.length - leadingCheckpoints - trailingCheckpoints);
	const collapsed = hiddenCount > 10;
	const leading = collapsed ? visible.slice(0, leadingCheckpoints) : visible;
	const trailing = collapsed ? visible.slice(-trailingCheckpoints) : [];

	if (steps.length === 0) {
		return (
			<EmptyState icon={ListTree} title={runFinished ? 'No checkpoints were recorded' : 'No checkpoints yet'}>
				{runFinished
					? 'The run finished without reaching a durable operation.'
					: 'Checkpoints appear as the run reaches each step, sleep, signal or child run.'}
			</EmptyState>
		);
	}

	return (
		<div>
			{steps.length > 6 && issues.length > 0 ? (
				<div className="mb-4 flex items-center gap-1.5" role="group" aria-label="Checkpoint filter">
					{[
						{ value: false, label: `All ${formatNumber(steps.length)}` },
						{ value: true, label: `Needs attention ${formatNumber(issues.length)}` },
					].map((option) => (
						<button
							key={option.label}
							type="button"
							aria-pressed={issuesOnly === option.value}
							onClick={() => setIssuesOnly(option.value)}
							className="press tabular inline-flex h-9 items-center rounded-full border border-line px-3 text-xs font-medium text-muted transition-[background-color,color,border-color,transform] aria-pressed:border-fg aria-pressed:bg-fg aria-pressed:text-page md:h-7"
						>
							{option.label}
						</button>
					))}
				</div>
			) : null}
			<ol aria-label="Checkpoints">
				{leading.map((step, index) => (
					<TimelineItem
						key={step.name}
						step={step}
						child={step.child_run_id ? childById.get(step.child_run_id) : undefined}
						last={!collapsed && index === leading.length - 1}
					/>
				))}
				{collapsed ? (
					<li className="relative grid grid-cols-[2rem_minmax(0,1fr)] gap-x-3 pb-5">
						<span aria-hidden className="absolute top-0 bottom-0 left-[0.9375rem] w-px border-l border-dashed border-line-strong" />
						<span />
						<button
							type="button"
							onClick={() => setShowAll(true)}
							className="press tabular min-h-10 w-fit rounded-lg border border-line bg-surface-2 px-3 text-[0.8125rem] font-medium text-muted transition-[background-color,color,transform] hover:bg-surface-3 hover:text-fg md:min-h-8"
						>
							Show {formatNumber(hiddenCount)} more checkpoints
						</button>
					</li>
				) : null}
				{trailing.map((step, index) => (
					<TimelineItem
						key={step.name}
						step={step}
						child={step.child_run_id ? childById.get(step.child_run_id) : undefined}
						last={index === trailing.length - 1}
					/>
				))}
			</ol>
		</div>
	);
}
