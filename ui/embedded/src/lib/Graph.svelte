<script lang="ts">
	import { SvelteFlow, Background, Controls, type Node, type Edge, BackgroundVariant } from '@xyflow/svelte';
	import '@xyflow/svelte/dist/style.css';
	import type { WorkflowRunGraphNode, WorkflowGraphEdge } from '$lib/types';
	import WorkflowNode from './WorkflowNode.svelte';

	let { nodes: rawNodes, edges: rawEdges } = $props<{ 
		nodes: WorkflowRunGraphNode[], 
		edges: WorkflowGraphEdge[] 
	}>();

	const nodeTypes = {
		workflowNode: WorkflowNode
	};

	// Basic DAG layout (top-down) using simple topological sort to calculate ranks
	function calculateLayout(nodes: WorkflowRunGraphNode[], edges: WorkflowGraphEdge[]) {
		const result: Node[] = [];
		const resultEdges: Edge[] = [];
		
		// Build adjacency list & indegrees
		const adj = new Map<string, string[]>();
		const indegree = new Map<string, number>();
		
		nodes.forEach(n => {
			adj.set(n.node.id, []);
			indegree.set(n.node.id, 0);
		});
		
		edges.forEach(e => {
			if (adj.has(e.from) && indegree.has(e.to)) {
				adj.get(e.from)!.push(e.to);
				indegree.set(e.to, indegree.get(e.to)! + 1);
			}
			
			resultEdges.push({
				id: `${e.from}-${e.to}`,
				source: e.from,
				target: e.to,
				animated: true,
				style: 'stroke: var(--color-surface-400); stroke-width: 2px;'
			});
		});

		// Calculate ranks (topological levels)
		const ranks = new Map<string, number>();
		const queue: string[] = [];
		
		for (const [id, deg] of indegree.entries()) {
			if (deg === 0) {
				queue.push(id);
				ranks.set(id, 0);
			}
		}

		while (queue.length > 0) {
			const curr = queue.shift()!;
			const currentRank = ranks.get(curr)!;
			
			for (const next of (adj.get(curr) || [])) {
				indegree.set(next, indegree.get(next)! - 1);
				ranks.set(next, Math.max(ranks.get(next) || 0, currentRank + 1));
				
				if (indegree.get(next) === 0) {
					queue.push(next);
				}
			}
		}

		// Group nodes by rank to calculate X positions
		const nodesByRank = new Map<number, WorkflowRunGraphNode[]>();
		nodes.forEach(n => {
			const r = ranks.get(n.node.id) || 0;
			if (!nodesByRank.has(r)) nodesByRank.set(r, []);
			nodesByRank.get(r)!.push(n);
		});

		// Generate layout
		const NODE_WIDTH = 320;
		const X_SPACING = 380;
		const Y_SPACING = 200;

		for (const [rank, rankNodes] of nodesByRank.entries()) {
			const totalWidth = (rankNodes.length - 1) * X_SPACING;
			const startX = -totalWidth / 2;
			
			rankNodes.forEach((n, idx) => {
				const attemptText = n.step ? `Attempt ${n.step.attempt}/${n.step.max_attempts}` : 'Not started';

				result.push({
					id: n.node.id,
					type: 'workflowNode',
					position: { x: startX + idx * X_SPACING, y: rank * Y_SPACING },
					data: { 
						kind: n.node.kind,
						label: n.node.label,
						id: n.node.id,
						status: n.status,
						attemptText,
						fanout: n.item_counts,
						onRetry: () => {
							console.log('Retry clicked for node', n.node.id);
							// TODO: integrate API retry call here
						}
					}
				});
			});
		}

		return { flowNodes: result, flowEdges: resultEdges };
	}

	let flowNodes = $state<Node[]>([]);
	let flowEdges = $state<Edge[]>([]);

	$effect(() => {
		const layout = calculateLayout(rawNodes, rawEdges);
		flowNodes = layout.flowNodes;
		flowEdges = layout.flowEdges;
	});
</script>

<div class="h-[600px] w-full border border-surface-200/60 rounded-3xl overflow-hidden bg-surface-50 relative shadow-inner">
	<SvelteFlow 
		nodes={flowNodes} 
		{nodeTypes}
		edges={flowEdges} 
		fitView
		minZoom={0.2}
		maxZoom={1.5}
	>
		<Background variant={BackgroundVariant.Dots} gap={20} size={1} />
		<Controls />
	</SvelteFlow>
</div>