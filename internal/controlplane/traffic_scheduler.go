package controlplane

import (
	"container/heap"
	"fmt"
	"math"
	"sync"
	"time"
)

// TrafficScheduler 基于UMW的流量调度器
type TrafficScheduler struct {
	config      *UMWConfig
	graph       *ExtendedGraph
	globalState *GlobalNetworkState
	mu          sync.RWMutex

	// 统计信息
	totalRequests  int64
	totalResponses int64
	averageLatency float64
	lastUpdateTime time.Time
}

// NewTrafficScheduler 创建新的流量调度器
func NewTrafficScheduler(config *UMWConfig) *TrafficScheduler {
	if config == nil {
		config = DefaultUMWConfig()
	}

	// 验证配置参数
	if err := ValidateUMWConfig(config); err != nil {
		// 如果配置无效，使用默认配置并记录警告
		fmt.Printf("Warning: Invalid UMW config (%v), using default config\n", err)
		config = DefaultUMWConfig()
	}

	return &TrafficScheduler{
		config:         config,
		graph:          NewExtendedGraph(),
		lastUpdateTime: time.Now(),
	}
}

// NewExtendedGraph 创建新的扩展图
func NewExtendedGraph() *ExtendedGraph {
	return &ExtendedGraph{
		Nodes: make(map[string]*ExtendedNode),
		Edges: make(map[string]*ExtendedEdge),
	}
}

// UpdateGlobalState 更新全局网络状态
func (ts *TrafficScheduler) UpdateGlobalState(state *GlobalNetworkState) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.globalState = state
	ts.lastUpdateTime = time.Now()

	// 重建扩展图
	ts.rebuildExtendedGraph()
}

// rebuildExtendedGraph 根据全局状态重建扩展图
func (ts *TrafficScheduler) rebuildExtendedGraph() {
	ts.graph.mu.Lock()
	defer ts.graph.mu.Unlock()

	// 清空现有图
	ts.graph.Nodes = make(map[string]*ExtendedNode)
	ts.graph.Edges = make(map[string]*ExtendedEdge)

	if ts.globalState == nil {
		return
	}

	// 为每个物理节点创建输入和输出组件
	allNodes := make(map[string]*NodeStatus)

	// 收集本地区域节点
	for nodeID, nodeStatus := range ts.globalState.LocalNodes {
		allNodes[nodeID] = &nodeStatus
	}

	// 收集远程区域节点（从摘要中推断）
	for regionID := range ts.globalState.RemoteRegions {
		// 为远程区域创建代表性节点
		virtualNodeID := fmt.Sprintf("virtual_%s", regionID)
		allNodes[virtualNodeID] = &NodeStatus{
			NodeID:    virtualNodeID,
			Region:    regionID,
			Address:   fmt.Sprintf("region_%s", regionID),
			IsHealthy: true,
		}
	}

	// 为每个节点创建输入和输出组件
	for nodeID, nodeStatus := range allNodes {
		inputNodeID := fmt.Sprintf("%s_in", nodeID)
		outputNodeID := fmt.Sprintf("%s_out", nodeID)

		// 创建输入节点
		ts.graph.Nodes[inputNodeID] = &ExtendedNode{
			NodeID:       inputNodeID,
			InputNodeID:  inputNodeID,
			OutputNodeID: outputNodeID,
			Region:       nodeStatus.Region,
			Address:      nodeStatus.Address,
			IsVirtual:    true,
		}

		// 创建输出节点
		ts.graph.Nodes[outputNodeID] = &ExtendedNode{
			NodeID:       outputNodeID,
			InputNodeID:  inputNodeID,
			OutputNodeID: outputNodeID,
			Region:       nodeStatus.Region,
			Address:      nodeStatus.Address,
			IsVirtual:    true,
		}

		// 创建节点内部虚拟边 (iv -> jo)
		virtualEdgeID := fmt.Sprintf("%s_virtual", nodeID)
		ts.graph.Edges[virtualEdgeID] = &ExtendedEdge{
			EdgeID:        virtualEdgeID,
			SourceNodeID:  inputNodeID,
			TargetNodeID:  outputNodeID,
			Capacity:      math.Inf(1), // 节点内部容量无限
			UnitCost:      0.0,         // 节点内部成本为0
			VirtualQueue:  0.0,         // 初始虚拟队列为0
			IsVirtualEdge: true,
			LastUpdated:   time.Now().Unix(),
		}
	}

	// 创建节点间的物理链路
	ts.createPhysicalLinks(allNodes)
}

// createPhysicalLinks 创建节点间的物理链路
func (ts *TrafficScheduler) createPhysicalLinks(nodes map[string]*NodeStatus) {
	nodeList := make([]*NodeStatus, 0, len(nodes))
	for _, node := range nodes {
		nodeList = append(nodeList, node)
	}

	// 为每对节点创建双向链路
	for i, sourceNode := range nodeList {
		for j, targetNode := range nodeList {
			if i == j {
				continue // 跳过自己到自己的链路
			}

			sourceOutputID := fmt.Sprintf("%s_out", sourceNode.NodeID)
			targetInputID := fmt.Sprintf("%s_in", targetNode.NodeID)

			edgeID := fmt.Sprintf("%s_to_%s", sourceNode.NodeID, targetNode.NodeID)

			// 计算链路容量和成本
			capacity := ts.estimateLinkCapacity(sourceNode, targetNode)
			unitCost := ts.estimateLinkCost(sourceNode, targetNode)
			virtualQueue := ts.getVirtualQueueValue(sourceNode, targetNode)

			ts.graph.Edges[edgeID] = &ExtendedEdge{
				EdgeID:        edgeID,
				SourceNodeID:  sourceOutputID,
				TargetNodeID:  targetInputID,
				Capacity:      capacity,
				UnitCost:      unitCost,
				VirtualQueue:  virtualQueue,
				IsVirtualEdge: false,
				LastUpdated:   time.Now().Unix(),
			}
		}
	}
}

// estimateLinkCapacity 估算链路容量
func (ts *TrafficScheduler) estimateLinkCapacity(source, target *NodeStatus) float64 {
	// 简化实现：基于节点的出站带宽
	if source.OutboundBandwidth > 0 {
		return float64(source.OutboundBandwidth)
	}
	return 1024 * 1024 // 默认1MB/s
}

// estimateLinkCost 估算链路成本
func (ts *TrafficScheduler) estimateLinkCost(source, target *NodeStatus) float64 {
	// 简化实现：同区域成本低，跨区域成本高
	if source.Region == target.Region {
		return 1.0 // 区域内成本
	}
	return 10.0 // 跨区域成本
}

// getVirtualQueueValue 获取虚拟队列值
func (ts *TrafficScheduler) getVirtualQueueValue(source, target *NodeStatus) float64 {
	// 从源节点的虚拟队列状态中获取
	linkID := fmt.Sprintf("link_to_%s", target.NodeID)
	if queue, exists := source.VirtualQueues[linkID]; exists {
		return queue
	}
	return 0.0 // 默认值
}

// ComputeOptimalRoute 计算最优路由 (实现UMW算法)
func (ts *TrafficScheduler) ComputeOptimalRoute(request *RoutingRequest) (*RoutingResponse, error) {
	startTime := time.Now()

	// 验证输入参数
	if err := ValidateRoutingRequest(request); err != nil {
		return nil, fmt.Errorf("invalid routing request: %v", err)
	}

	ts.mu.RLock()
	defer ts.mu.RUnlock()

	if ts.globalState == nil {
		return nil, fmt.Errorf("global state not available")
	}

	// 检查网络状态是否过期
	if time.Since(ts.lastUpdateTime) > 30*time.Second {
		return nil, fmt.Errorf("global state is stale (last update: %v)", ts.lastUpdateTime)
	}

	// 第一阶段：路由 (Routing)
	path, pathCost, err := ts.computeShortestPath(request.SourceID, request.DestinationID)
	if err != nil {
		return nil, fmt.Errorf("failed to compute shortest path: %v", err)
	}

	// 第二阶段：分配 (Allocation)
	recommendedRate := ts.computeOptimalRate(pathCost, request.Priority, request.FairnessAlpha)

	// 应用带宽约束
	recommendedRate = ts.applyBandwidthConstraints(path, recommendedRate)

	computationTime := float64(time.Since(startTime).Nanoseconds()) / 1e6 // 转换为毫秒

	response := &RoutingResponse{
		TaskID:           request.TaskID,
		OptimalPath:      ts.convertToPhysicalPath(path),
		RecommendedRate:  recommendedRate,
		PathCost:         pathCost,
		ComputationTime:  computationTime,
		AlgorithmVersion: "UMW-v1.0",
		Timestamp:        time.Now().Unix(),
	}

	// 更新虚拟队列
	ts.updateVirtualQueues(path, recommendedRate)

	// 更新统计信息
	ts.totalRequests++
	ts.totalResponses++
	ts.averageLatency = (ts.averageLatency*float64(ts.totalResponses-1) + computationTime) / float64(ts.totalResponses)

	return response, nil
}

// computeShortestPath 使用Dijkstra算法计算最短路径
func (ts *TrafficScheduler) computeShortestPath(sourceID, destinationID string) ([]string, float64, error) {
	ts.graph.mu.RLock()
	defer ts.graph.mu.RUnlock()

	// 转换为扩展图中的节点ID
	sourceNodeID := fmt.Sprintf("%s_out", sourceID)
	destNodeID := fmt.Sprintf("%s_in", destinationID)

	// 检查源和目标节点是否存在
	if _, exists := ts.graph.Nodes[sourceNodeID]; !exists {
		return nil, 0, fmt.Errorf("source node %s not found in network topology", sourceID)
	}
	if _, exists := ts.graph.Nodes[destNodeID]; !exists {
		return nil, 0, fmt.Errorf("destination node %s not found in network topology", destinationID)
	}

	// 检查图的连通性
	if len(ts.graph.Nodes) == 0 {
		return nil, 0, fmt.Errorf("network topology is empty")
	}
	if len(ts.graph.Edges) == 0 {
		return nil, 0, fmt.Errorf("no network links available")
	}

	// Dijkstra算法实现
	distances := make(map[string]float64)
	previous := make(map[string]string)
	visited := make(map[string]bool)

	// 初始化距离
	for nodeID := range ts.graph.Nodes {
		distances[nodeID] = math.Inf(1)
	}
	distances[sourceNodeID] = 0

	// 优先队列
	pq := &PriorityQueue{}
	heap.Init(pq)
	heap.Push(pq, &Item{
		NodeID:   sourceNodeID,
		Priority: 0,
	})

	// 添加迭代计数器防止无限循环
	maxIterations := ts.config.MaxIterations
	iterations := 0

	for pq.Len() > 0 && iterations < maxIterations {
		iterations++
		current := heap.Pop(pq).(*Item)
		currentNodeID := current.NodeID

		if visited[currentNodeID] {
			continue
		}
		visited[currentNodeID] = true

		if currentNodeID == destNodeID {
			break
		}

		// 检查所有出边
		edgeCount := 0
		for _, edge := range ts.graph.Edges {
			if edge.SourceNodeID != currentNodeID {
				continue
			}
			edgeCount++

			neighborID := edge.TargetNodeID
			if visited[neighborID] {
				continue
			}

			// 验证边的有效性
			if edge.Capacity <= 0 {
				continue // 跳过容量为0的边
			}

			// 计算边权重 (UMW公式: We = w_e^M * Õe(t) + V_M * p_e)
			weight := ts.config.WeightM*edge.VirtualQueue + ts.config.PenaltyV*edge.UnitCost

			// 检查权重是否有效
			if math.IsInf(weight, 0) || math.IsNaN(weight) || weight < 0 {
				continue // 跳过无效权重的边
			}

			newDistance := distances[currentNodeID] + weight
			if newDistance < distances[neighborID] {
				distances[neighborID] = newDistance
				previous[neighborID] = currentNodeID

				heap.Push(pq, &Item{
					NodeID:   neighborID,
					Priority: newDistance,
				})
			}
		}
	}

	// 检查是否因为迭代次数限制而退出
	if iterations >= maxIterations {
		return nil, 0, fmt.Errorf("shortest path algorithm exceeded maximum iterations (%d)", maxIterations)
	}

	// 重建路径
	if distances[destNodeID] == math.Inf(1) {
		return nil, 0, fmt.Errorf("no path found from %s to %s", sourceID, destinationID)
	}

	path := []string{}
	current := destNodeID
	for current != "" {
		path = append([]string{current}, path...)
		current = previous[current]
	}

	return path, distances[destNodeID], nil
}

// computeOptimalRate 计算最优传输速率
func (ts *TrafficScheduler) computeOptimalRate(pathCost, priority, alpha float64) float64 {
	// 输入验证
	if pathCost <= 0 {
		return 0
	}
	if priority <= 0 {
		return 0
	}
	if alpha < 0 || alpha > 1 {
		alpha = ts.config.DefaultAlpha // 使用默认值
	}

	// UMW公式: a*k(t) = (V_M * w_k^U / C*k(t))^(1/α)
	if alpha == 0 {
		// 吞吐量最大化情况
		rate := ts.config.PenaltyV * priority / pathCost
		if math.IsInf(rate, 0) || math.IsNaN(rate) {
			return 0
		}
		return rate
	}

	// 一般情况
	base := ts.config.PenaltyV * priority / pathCost
	if base <= 0 {
		return 0
	}

	rate := math.Pow(base, 1.0/alpha)

	// 检查结果的有效性
	if math.IsInf(rate, 0) || math.IsNaN(rate) || rate < 0 {
		return 0
	}

	// 应用合理的上限（防止过大的速率）
	maxRate := 1e9 // 1GB/s
	if rate > maxRate {
		rate = maxRate
	}

	return rate
}

// applyBandwidthConstraints 应用带宽约束
func (ts *TrafficScheduler) applyBandwidthConstraints(path []string, rate float64) float64 {
	ts.graph.mu.RLock()
	defer ts.graph.mu.RUnlock()

	minCapacity := math.Inf(1)

	// 找到路径上的最小容量
	for i := 0; i < len(path)-1; i++ {
		sourceNodeID := path[i]
		targetNodeID := path[i+1]

		// 查找对应的边
		for _, edge := range ts.graph.Edges {
			if edge.SourceNodeID == sourceNodeID && edge.TargetNodeID == targetNodeID {
				if edge.Capacity < minCapacity {
					minCapacity = edge.Capacity
				}
				break
			}
		}
	}

	if minCapacity == math.Inf(1) {
		return rate
	}

	// 应用容量约束
	if rate > minCapacity {
		return minCapacity
	}

	return rate
}

// convertToPhysicalPath 将扩展图路径转换为物理节点路径
func (ts *TrafficScheduler) convertToPhysicalPath(extendedPath []string) []string {
	physicalPath := []string{}

	for _, nodeID := range extendedPath {
		// 提取物理节点ID
		if node, exists := ts.graph.Nodes[nodeID]; exists {
			// 从输入或输出节点ID中提取原始节点ID
			originalNodeID := ts.extractOriginalNodeID(node.NodeID)
			if originalNodeID != "" && !ts.containsString(physicalPath, originalNodeID) {
				physicalPath = append(physicalPath, originalNodeID)
			}
		}
	}

	return physicalPath
}

// extractOriginalNodeID 从扩展节点ID中提取原始节点ID
func (ts *TrafficScheduler) extractOriginalNodeID(extendedNodeID string) string {
	if len(extendedNodeID) > 4 && extendedNodeID[len(extendedNodeID)-4:] == "_out" {
		return extendedNodeID[:len(extendedNodeID)-4]
	}
	if len(extendedNodeID) > 3 && extendedNodeID[len(extendedNodeID)-3:] == "_in" {
		return extendedNodeID[:len(extendedNodeID)-3]
	}
	return extendedNodeID
}

// containsString 检查字符串切片是否包含指定字符串
func (ts *TrafficScheduler) containsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// updateVirtualQueues 更新虚拟队列状态
func (ts *TrafficScheduler) updateVirtualQueues(path []string, allocatedRate float64) {
	ts.graph.mu.Lock()
	defer ts.graph.mu.Unlock()

	// 输入验证
	if len(path) < 2 {
		return // 路径太短，无需更新
	}
	if allocatedRate < 0 {
		return // 分配速率不能为负
	}

	// 根据公式 Õe(t+1) = max[0, Õe(t) + ae(t) - be(t)] 更新虚拟队列
	updatedCount := 0
	for i := 0; i < len(path)-1; i++ {
		sourceNodeID := path[i]
		targetNodeID := path[i+1]

		// 查找对应的边并更新虚拟队列
		edgeFound := false
		for _, edge := range ts.graph.Edges {
			if edge.SourceNodeID == sourceNodeID && edge.TargetNodeID == targetNodeID {
				// ae(t) = allocatedRate, be(t) = edge.Capacity
				oldQueue := edge.VirtualQueue
				newQueue := oldQueue + allocatedRate - edge.Capacity

				// 确保虚拟队列值非负
				if newQueue < 0 {
					newQueue = 0
				}

				// 应用合理的上限防止队列值过大
				maxQueue := 1000.0
				if newQueue > maxQueue {
					newQueue = maxQueue
				}

				edge.VirtualQueue = newQueue
				edge.LastUpdated = time.Now().Unix()
				updatedCount++
				edgeFound = true
				break
			}
		}

		// 如果找不到对应的边，记录警告（在实际部署中可能需要日志）
		if !edgeFound {
			// 这里可以添加日志记录
			continue
		}
	}

	// 可以添加统计信息更新
	if updatedCount > 0 {
		ts.lastUpdateTime = time.Now()
	}
}

// GetStatistics 获取调度器统计信息
func (ts *TrafficScheduler) GetStatistics() map[string]interface{} {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	return map[string]interface{}{
		"total_requests":   ts.totalRequests,
		"total_responses":  ts.totalResponses,
		"average_latency":  ts.averageLatency,
		"last_update_time": ts.lastUpdateTime,
		"graph_nodes":      len(ts.graph.Nodes),
		"graph_edges":      len(ts.graph.Edges),
	}
}

// 优先队列实现 (用于Dijkstra算法)

// Item 优先队列中的项目
type Item struct {
	NodeID   string  // 节点ID
	Priority float64 // 优先级 (距离)
	Index    int     // 在堆中的索引
}

// PriorityQueue 优先队列
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Priority < pq[j].Priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}
