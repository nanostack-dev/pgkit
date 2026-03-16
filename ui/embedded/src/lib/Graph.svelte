<script lang="ts">
	import dagre from '@dagrejs/dagre';
	import {
		Background,
		BackgroundVariant,
		Controls,
		MarkerType,
		Position,
		SvelteFlow,
		type Edge,
		type Node
	} from '@xyflow/svelte';
	import '@xyflow/svelte/dist/style.css';
	import type { WorkflowGraphEdge, WorkflowRunGraphNode } from '$lib/types';
	import WorkflowNode from './WorkflowNode.svelte';

	let { nodes: rawNodes, edges: rawEdges } = $props<{
		nodes: WorkflowRunGraphNode[];
		edges: WorkflowGraphEdge[];
	}>();

	const nodeTypes = {
		workflowNode: WorkflowNode
	};

	const NODE_WIDTH = 348;
	const NODE_HEIGHT = 188;
	const RANK_SEPARATION = 160;
	const SIBLING_SEPARATION = 120;
	const EDGE_PADDING = 20;

	function dagreLayout(nodes: WorkflowRunGraphNode[], edges: WorkflowGraphEdge[]) {
		const graph = new dagre.graphlib.Graph({ multigraph: false, compound: false });
		graph.setDefaultEdgeLabel(() => ({}));
		graph.setGraph({
			rankdir: 'TB',
			align: 'UL',
			ranksep: RANK_SEPARATION,
			nodesep: SIBLING_SEPARATION,
			edgesep: EDGE_PADDING,
			marginx: 24,
			marginy: 24,
			acyclicer: 'greedy',
			ranker: 'tight-tree'
		});

		for (const entry of nodes) {
			const fanoutRows = entry.item_counts.total > 0 ? Math.ceil(entry.item_counts.total / 4) : 0;
			const derivedHeight = NODE_HEIGHT + fanoutRows * 10;
			graph.setNode(entry.node.id, {
				width: NODE_WIDTH,
				height: derivedHeight
			});
		}

		for (const edge of edges) {
			graph.setEdge(edge.from, edge.to);
		}

		dagre.layout(graph);

		const incomingCounts = new Map<string, number>();
		const outgoingCounts = new Map<string, number>();
		for (const edge of edges) {
			outgoingCounts.set(edge.from, (outgoingCounts.get(edge.from) ?? 0) + 1);
			incomingCounts.set(edge.to, (incomingCounts.get(edge.to) ?? 0) + 1);
		}

		const flowNodes: Node[] = nodes.map((entry) => {
			const layoutNode = graph.node(entry.node.id);
			const hasIncoming = (incomingCounts.get(entry.node.id) ?? 0) > 0;
			const hasOutgoing = (outgoingCounts.get(entry.node.id) ?? 0) > 0;
			const attemptText = entry.step
				? `Attempt ${entry.step.attempt}/${entry.step.max_attempts}`
				: entry.item_counts.total > 0
					? `${entry.item_counts.total} fan-out items`
					: 'Awaiting execution';

			return {
				id: entry.node.id,
				type: 'workflowNode',
				targetPosition: hasIncoming ? Position.Top : Position.Left,
				sourcePosition: hasOutgoing ? Position.Bottom : Position.Right,
				position: {
					x: layoutNode.x - layoutNode.width / 2,
					y: layoutNode.y - layoutNode.height / 2
				},
				data: {
					kind: entry.node.kind,
					label: entry.node.label,
					id: entry.node.id,
					status: entry.status,
					attemptText,
					fanout: entry.item_counts,
					dependsOn: entry.node.depends_on ?? [],
					queue: entry.node.queue ?? null,
					maxAttempts: entry.node.max_attempts ?? entry.step?.max_attempts ?? 0,
					hasIncoming,
					hasOutgoing
				}
			};
		});

		const flowEdges: Edge[] = edges.map((edge) => ({
			id: `${edge.from}-${edge.to}`,
			source: edge.from,
			target: edge.to,
			type: 'smoothstep',
			animated: false,
			markerEnd: {
				type: MarkerType.ArrowClosed,
				width: 18,
				height: 18,
				color: 'var(--color-surface-500)'
			},
			style: 'stroke: var(--color-surface-500); stroke-width: 2px;'
		}));

		return { flowNodes, flowEdges };
	}

	let flowNodes = $state<Node[]>([]);
	let flowEdges = $state<Edge[]>([]);

	$effect(() => {
		const layout = dagreLayout(rawNodes, rawEdges);
		flowNodes = layout.flowNodes;
		flowEdges = layout.flowEdges;
	});
</script>

<div class="relative h-[780px] w-full overflow-hidden rounded-[2rem] border border-surface-200/60 bg-[radial-gradient(circle_at_top,rgba(255,255,255,0.92),rgba(240,244,249,0.88)_40%,rgba(229,236,243,0.9)_100%)] shadow-inner">
	<SvelteFlow
		nodes={flowNodes}
		{nodeTypes}
		edges={flowEdges}
		fitView
		fitViewOptions={{ padding: 0.14, minZoom: 0.3, maxZoom: 1.1 }}
		minZoom={0.15}
		maxZoom={1.6}
		proOptions={{ hideAttribution: true }}
	>
		<Background variant={BackgroundVariant.Dots} gap={20} size={1.1} />
		<Controls position="top-right" />
	</SvelteFlow>
</div>
