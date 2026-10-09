import { Inbox, Workflow } from 'lucide-react';
import { useSearchParams } from 'react-router';
import { useConfig, useRuns, useWorkflowStats } from '../api/queries';
import { runStatuses, type RunStatus, type WorkflowStats } from '../api/types';
import { Button } from '../components/Button';
import { Pagination, SearchInput, SegmentedFilter, SelectField, SwitchField } from '../components/Filters';
import { LiveIndicator, PageHeader, Section } from '../components/Layout';
import { RunRow } from '../components/RunRow';
import { EmptyState, ErrorState, ListSkeleton } from '../components/States';
import { RelativeTime } from '../components/Time';
import { cn } from '../lib/cn';
import { formatCompact, formatNumber } from '../lib/format';
import { statusLabel, toneDot } from '../lib/status';

function readStatus(value: string | null): RunStatus | undefined {
	return runStatuses.includes(value as RunStatus) ? (value as RunStatus) : undefined;
}

function WorkflowCard({ stats, selected, onSelect }: { stats: WorkflowStats; selected: boolean; onSelect: () => void }) {
	const segments: { key: RunStatus; tone: keyof typeof toneDot }[] = [
		{ key: 'running', tone: 'run' },
		{ key: 'waiting', tone: 'wait' },
		{ key: 'failed', tone: 'bad' },
	];
	return (
		<li className="w-[15rem] shrink-0 snap-start md:w-auto">
			<button
				type="button"
				aria-pressed={selected}
				onClick={onSelect}
				className={cn(
					'press flex h-full w-full min-w-0 flex-col gap-2 rounded-xl border bg-surface p-3.5 text-left transition-[background-color,border-color,transform] hover:bg-surface-2',
					selected ? 'border-fg' : 'border-line',
				)}
			>
				<span className="flex w-full min-w-0 items-baseline justify-between gap-2">
					<span className="truncate text-[0.8125rem] font-medium" title={stats.workflow}>
						{stats.workflow}
					</span>
					<span className="tabular shrink-0 text-xs text-muted">{formatCompact(stats.total)}</span>
				</span>
				<span className="tabular flex flex-wrap gap-x-3 gap-y-1 text-xs text-muted">
					{segments.map((segment) =>
						stats[segment.key] > 0 ? (
							<span key={segment.key} className="inline-flex items-center gap-1.5">
								<span aria-hidden className={cn('size-1.5 rounded-full', toneDot[segment.tone])} />
								{formatNumber(stats[segment.key])} {statusLabel(segment.key).toLowerCase()}
							</span>
						) : null,
					)}
					<span className="inline-flex items-center gap-1.5">
						<span aria-hidden className={cn('size-1.5 rounded-full', toneDot.ok)} />
						{formatNumber(stats.succeeded)} succeeded
					</span>
				</span>
				<span className="flex items-center justify-between gap-2 text-xs text-subtle">
					<span className="truncate">
						{stats.versions.length > 1
							? `versions ${stats.versions.join(', ')}`
							: stats.versions[0]
								? `version ${stats.versions[0]}`
								: 'unversioned'}
					</span>
					<RelativeTime value={stats.last_run_at} className="shrink-0" />
				</span>
			</button>
		</li>
	);
}

export function WorkflowsPage() {
	const [params, setParams] = useSearchParams();
	const config = useConfig();
	const workflowsEnabled = config.data?.workflows_enabled ?? true;
	const pageSize = config.data?.page_size ?? 20;
	const stats = useWorkflowStats(workflowsEnabled);

	const workflow = params.get('workflow') ?? undefined;
	const status = readStatus(params.get('status'));
	const search = params.get('search') ?? '';
	const showChildren = params.get('children') === '1';
	const offset = Math.max(0, Number(params.get('offset') ?? 0) || 0);

	const runs = useRuns(
		{
			workflow,
			status,
			search: search || undefined,
			top_level: showChildren ? undefined : true,
			limit: pageSize,
			offset,
		},
		workflowsEnabled,
	);

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

	const statItems = stats.data?.items ?? [];
	const countSource = workflow ? statItems.filter((item) => item.workflow === workflow) : statItems;
	const counts = Object.fromEntries(
		runStatuses.map((value) => [value, countSource.reduce((sum, item) => sum + item[value], 0)]),
	) as Record<RunStatus, number>;
	const hasFilters = Boolean(workflow || status || search);

	if (config.data && !workflowsEnabled) {
		return (
			<>
				<PageHeader title="Workflows" />
				<div className="card">
					<EmptyState icon={Workflow} title="Workflows are not configured">
						This admin server was started without a workflow client. Pass <code className="font-mono">Options.Workflow</code> to{' '}
						<code className="font-mono">adminui.New</code> to inspect durable runs.
					</EmptyState>
				</div>
			</>
		);
	}

	return (
		<>
			<PageHeader
				title="Workflows"
				description="Durable runs, their checkpoints and what they are waiting for."
				actions={<LiveIndicator updatedAt={runs.dataUpdatedAt} error={runs.isError && Boolean(runs.data)} />}
			/>

			{statItems.length > 0 ? (
				<section aria-labelledby="workflow-list-title" className="mb-6">
					<div className="mb-2.5 flex items-center justify-between gap-3">
						<h2 id="workflow-list-title" className="text-[0.8125rem] font-semibold text-muted">
							Workflows
						</h2>
						{workflow ? (
							<button type="button" onClick={() => update({ workflow: undefined })} className="min-h-8 text-xs font-medium text-muted hover:text-fg">
								Show all workflows
							</button>
						) : null}
					</div>
					<ul className="no-scrollbar -mx-4 flex snap-x snap-mandatory gap-3 overflow-x-auto scroll-px-4 px-4 pb-1 md:mx-0 md:grid md:grid-cols-2 md:overflow-visible md:px-0 xl:grid-cols-3">
						{statItems.map((item) => (
							<WorkflowCard
								key={item.workflow}
								stats={item}
								selected={item.workflow === workflow}
								onSelect={() => update({ workflow: item.workflow === workflow ? undefined : item.workflow })}
							/>
						))}
					</ul>
				</section>
			) : null}

			<Section title="Runs" description={workflow ? `Runs of ${workflow}` : 'Newest first'} flush>
				<div className="flex flex-col gap-2.5 border-b border-line p-3 md:p-4">
					<div className="flex flex-col gap-2.5 lg:flex-row lg:items-center">
						<SearchInput
							value={search}
							onChange={(value) => update({ search: value })}
							label="Search runs"
							placeholder="Search run ID, key or workflow"
							className="lg:flex-1"
						/>
						<SelectField
							label="Workflow"
							value={workflow}
							placeholder="All workflows"
							options={statItems.map((item) => ({ value: item.workflow, label: item.workflow, hint: formatCompact(item.total) }))}
							onChange={(value) => update({ workflow: value })}
							className="lg:w-56"
						/>
						<SwitchField
							label="Include child runs"
							checked={showChildren}
							onChange={(checked) => update({ children: checked ? '1' : undefined })}
							className="lg:pl-1"
						/>
					</div>
					<SegmentedFilter
						label="Run status"
						value={status}
						options={runStatuses.map((value) => ({
							value,
							label: statusLabel(value),
							count: stats.data && showChildren ? counts[value] : undefined,
						}))}
						onChange={(value) => update({ status: value })}
					/>
				</div>
				{runs.isPending ? (
					<ListSkeleton label="Loading runs" />
				) : runs.isError && !runs.data ? (
					<ErrorState error={runs.error} onRetry={() => runs.refetch()} />
				) : runs.data && runs.data.items.length === 0 ? (
					<EmptyState
						icon={Inbox}
						title={hasFilters ? 'No runs match these filters' : 'No runs yet'}
						action={
							hasFilters ? (
								<Button size="sm" onClick={() => update({ workflow: undefined, status: undefined, search: undefined })}>
									Clear filters
								</Button>
							) : undefined
						}
					>
						{hasFilters
							? showChildren
								? 'Try another status or a broader search.'
								: 'Child runs are hidden. Include them or broaden the search.'
							: 'Start a workflow run and it will show up here.'}
					</EmptyState>
				) : runs.data ? (
					<ul className={cn('divide-y divide-line transition-opacity', runs.isPlaceholderData && 'opacity-60')} aria-busy={runs.isPlaceholderData}>
						{runs.data.items.map((run) => (
							<RunRow key={run.id} run={run} showParent={showChildren} />
						))}
					</ul>
				) : null}
				{runs.data ? (
					<div className="border-t border-line px-3 pb-3 md:px-4">
						<Pagination
							noun="Runs"
							total={runs.data.total}
							limit={pageSize}
							offset={offset}
							onOffsetChange={(next) => update({ offset: next ? String(next) : undefined }, false)}
						/>
					</div>
				) : null}
			</Section>
		</>
	);
}
