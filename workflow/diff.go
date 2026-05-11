package workflow

import "sort"

func diffGraphs(from, to Graph) *GraphDiff {
	fromNodes := make(map[string]GraphNode, len(from.Nodes))
	toNodes := make(map[string]GraphNode, len(to.Nodes))
	for _, node := range from.Nodes {
		fromNodes[node.ID] = node
	}
	for _, node := range to.Nodes {
		toNodes[node.ID] = node
	}
	diff := &GraphDiff{
		WorkflowName: from.Name,
		FromVersion:  from.Version,
		ToVersion:    to.Version,
	}
	for id, node := range fromNodes {
		other, ok := toNodes[id]
		if !ok {
			diff.RemovedNodes = append(diff.RemovedNodes, id)
			continue
		}
		if node.Kind != other.Kind || node.Label != other.Label || node.Queue != other.Queue || node.MaxAttempts != other.MaxAttempts || !sameStrings(node.DependsOn, other.DependsOn) {
			diff.ChangedNodes = append(diff.ChangedNodes, id)
		}
	}
	for id := range toNodes {
		if _, ok := fromNodes[id]; !ok {
			diff.AddedNodes = append(diff.AddedNodes, id)
		}
	}
	fromEdges := make(map[string]GraphEdge, len(from.Edges))
	toEdges := make(map[string]GraphEdge, len(to.Edges))
	for _, edge := range from.Edges {
		fromEdges[edgeKey(edge)] = edge
	}
	for _, edge := range to.Edges {
		toEdges[edgeKey(edge)] = edge
	}
	for key, edge := range fromEdges {
		if _, ok := toEdges[key]; !ok {
			diff.RemovedEdges = append(diff.RemovedEdges, edge)
		}
	}
	for key, edge := range toEdges {
		if _, ok := fromEdges[key]; !ok {
			diff.AddedEdges = append(diff.AddedEdges, edge)
		}
	}
	sort.Strings(diff.AddedNodes)
	sort.Strings(diff.RemovedNodes)
	sort.Strings(diff.ChangedNodes)
	sort.Slice(diff.AddedEdges, func(i, j int) bool { return edgeKey(diff.AddedEdges[i]) < edgeKey(diff.AddedEdges[j]) })
	sort.Slice(diff.RemovedEdges, func(i, j int) bool { return edgeKey(diff.RemovedEdges[i]) < edgeKey(diff.RemovedEdges[j]) })
	return diff
}

func edgeKey(edge GraphEdge) string {
	return edge.From + "->" + edge.To
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
