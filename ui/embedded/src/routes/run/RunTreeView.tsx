import { Link } from 'react-router';
import { useRunTree } from '../../api/queries';
import type { RunSummary, RunTreeNode } from '../../api/types';
import { ErrorState, Skeleton } from '../../components/States';
import { StatusBadge, StatusDot } from '../../components/StatusBadge';
import { cn } from '../../lib/cn';
import { formatNumber, plural, shortId } from '../../lib/format';

const maxIndent = 6;

function TreeRow({ node, current }: { node: RunTreeNode; current: boolean }) {
	const indent = Math.min(node.depth, maxIndent);
	const content = (
		<>
			<span aria-hidden className="flex shrink-0 items-center self-stretch" style={{ width: `${indent * 0.875}rem` }}>
				{Array.from({ length: indent }, (_, index) => (
					<span key={index} className="h-full w-[0.875rem] border-l border-line" />
				))}
			</span>
			<StatusDot status={node.status} />
			<span className="flex min-w-0 flex-1 flex-col py-2">
				<span className="flex min-w-0 items-baseline gap-1.5">
					<span className="truncate font-medium">{node.workflow}</span>
					{node.depth > maxIndent ? <span className="shrink-0 text-xs text-subtle">depth {node.depth}</span> : null}
					{node.child_count > 0 ? <span className="tabular shrink-0 text-xs text-subtle">{plural(node.child_count, 'child', 'children')}</span> : null}
				</span>
				<span className="truncate font-mono text-xs text-muted">{node.key ?? shortId(node.id)}</span>
			</span>
			<StatusBadge status={node.status} size="sm" className="hidden sm:inline-flex" />
		</>
	);
	if (current) {
		return (
			<li aria-current="page" className="flex min-h-11 items-center gap-2.5 rounded-lg bg-surface-2 px-2.5 text-[0.8125rem]">
				{content}
			</li>
		);
	}
	return (
		<li>
			<Link
				to={`/workflows/${encodeURIComponent(node.id)}`}
				data-row
				className="flex min-h-11 items-center gap-2.5 rounded-lg px-2.5 text-[0.8125rem] transition-colors hover:bg-surface-2"
			>
				{content}
			</Link>
		</li>
	);
}

export function RunTreeView({ runId, fallback, live }: { runId: string; fallback: RunSummary[]; live: boolean }) {
	const tree = useRunTree(runId, true, live);

	if (tree.isPending) {
		return (
			<div role="status" aria-label="Loading run tree" className="space-y-2">
				<Skeleton className="h-9 w-full" />
				<Skeleton className="ml-4 h-9 w-[calc(100%-1rem)]" />
				<Skeleton className="ml-4 h-9 w-[calc(100%-1rem)]" />
			</div>
		);
	}

	if (tree.isError) {
		if (fallback.length === 0) return <ErrorState error={tree.error} onRetry={() => tree.refetch()} className="py-6" />;
		return (
			<ul className="space-y-0.5">
				{fallback.map((child) => (
					<TreeRow
						key={child.id}
						current={false}
						node={{ ...child, depth: 1, child_count: child.child_count }}
					/>
				))}
			</ul>
		);
	}

	const nodes = tree.data.nodes;
	return (
		<div>
			<ul aria-label="Run tree" className={cn('space-y-0.5', nodes.length > 40 && 'max-h-[32rem] overflow-y-auto overscroll-contain')}>
				{nodes.map((node) => (
					<TreeRow key={node.id} node={node} current={node.id === runId} />
				))}
			</ul>
			{tree.data.truncated ? (
				<p className="mt-2 text-xs text-muted">
					Showing the first {formatNumber(nodes.length)} runs. Open a child to see its own subtree.
				</p>
			) : null}
		</div>
	);
}
