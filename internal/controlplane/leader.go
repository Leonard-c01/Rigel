package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/rigel/pkg/log"
)

// LeaderNode 组长节点实现
type LeaderNode struct {
	config *ControlPlaneConfig

	// 成员管理
	members map[string]*NodeMember // 区域内所有节点
	mu      sync.RWMutex

	// 全局状态视图
	globalState *GlobalNetworkState
	stateMu     sync.RWMutex

	// 其他区域摘要
	remoteRegions map[string]RegionSummary
	regionMu      sync.RWMutex

	// HTTP服务器
	httpServer *http.Server

	// UMW路由调度器
	trafficScheduler *TrafficScheduler

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewLeaderNode 创建组长节点
func NewLeaderNode(config *ControlPlaneConfig) *LeaderNode {
	if config == nil {
		config = DefaultControlPlaneConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &LeaderNode{
		config:           config,
		members:          make(map[string]*NodeMember),
		remoteRegions:    make(map[string]RegionSummary),
		trafficScheduler: NewTrafficScheduler(DefaultUMWConfig()),
		ctx:              ctx,
		cancel:           cancel,
		globalState: &GlobalNetworkState{
			LocalRegion:   config.Region,
			LocalNodes:    make(map[string]NodeStatus),
			RemoteRegions: make(map[string]RegionSummary),
		},
	}
}

// Start 启动组长节点
func (ln *LeaderNode) Start() error {
	log.Infof("Starting leader node for region: %s", ln.config.Region)

	// 启动HTTP API服务器
	if err := ln.startAPIServer(); err != nil {
		return fmt.Errorf("failed to start API server: %v", err)
	}

	// 启动后台任务
	ln.wg.Add(3)
	go ln.runHealthChecker()
	go ln.runGlobalStateUpdater()
	go ln.runLeaderSynchronizer()

	log.Infof("Leader node started successfully on %s", ln.config.APIPort)
	return nil
}

// Stop 停止组长节点
func (ln *LeaderNode) Stop() error {
	log.Infof("Stopping leader node...")

	ln.cancel()

	// 停止HTTP服务器
	if ln.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ln.httpServer.Shutdown(ctx)
	}

	ln.wg.Wait()
	log.Infof("Leader node stopped")
	return nil
}

// startAPIServer 启动API服务器
func (ln *LeaderNode) startAPIServer() error {
	mux := http.NewServeMux()

	// 注册API端点
	mux.HandleFunc("/api/v1/nodes/", ln.handleNodeStatusReport)
	mux.HandleFunc("/api/v1/global-status", ln.handleGlobalStatus)
	mux.HandleFunc("/api/v1/region-summary", ln.handleRegionSummary)
	mux.HandleFunc("/api/v1/region-nodes", ln.handleRegionNodes)
	mux.HandleFunc("/api/v1/routing/optimize", ln.handleRoutingOptimize)

	ln.httpServer = &http.Server{
		Addr:         ln.config.APIPort,
		Handler:      mux,
		ReadTimeout:  ln.config.RequestTimeout,
		WriteTimeout: ln.config.RequestTimeout,
	}

	go func() {
		if err := ln.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("API server error: %v", err)
		}
	}()

	return nil
}

// handleNodeStatusReport 处理节点状态上报
func (ln *LeaderNode) handleNodeStatusReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 从URL路径提取node_id
	nodeID := extractNodeIDFromPath(r.URL.Path)
	if nodeID == "" {
		http.Error(w, "Invalid node ID", http.StatusBadRequest)
		return
	}

	// 解析请求体
	var request StatusReportRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// 更新节点状态
	if err := ln.updateNodeStatus(nodeID, request.NodeStatus); err != nil {
		log.Errorf("Failed to update node status for %s: %v", nodeID, err)
		http.Error(w, "Failed to update status", http.StatusInternalServerError)
		return
	}

	// 返回成功响应
	response := StatusReportResponse{
		Success:   true,
		Message:   "Status updated successfully",
		Timestamp: time.Now(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

	log.Debugf("Status report received from node %s", nodeID)
}

// handleGlobalStatus 处理全局状态获取
func (ln *LeaderNode) handleGlobalStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 获取全局状态
	globalState := ln.getGlobalState()

	response := GlobalStatusResponse{
		GlobalState: *globalState,
		Success:     true,
		Message:     "Global status retrieved successfully",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

	log.Debugf("Global status requested")
}

// handleRegionSummary 处理区域摘要获取
func (ln *LeaderNode) handleRegionSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 生成区域摘要
	summary := ln.generateRegionSummary()

	response := RegionSummaryResponse{
		Summary: summary,
		Success: true,
		Message: "Region summary retrieved successfully",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

	log.Debugf("Region summary requested")
}

// handleRegionNodes 处理区域节点列表获取
func (ln *LeaderNode) handleRegionNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 获取区域节点列表
	nodes := ln.getRegionNodes()

	response := RegionNodesResponse{
		Nodes:   nodes,
		Success: true,
		Message: "Region nodes retrieved successfully",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

	log.Debugf("Region nodes requested")
}

// updateNodeStatus 更新节点状态
func (ln *LeaderNode) updateNodeStatus(nodeID string, status NodeStatus) error {
	ln.mu.Lock()
	defer ln.mu.Unlock()

	// 获取或创建节点成员
	member, exists := ln.members[nodeID]
	if !exists {
		member = &NodeMember{
			NodeID:   nodeID,
			Address:  status.Address,
			Region:   status.Region,
			JoinedAt: time.Now(),
		}
		ln.members[nodeID] = member
		log.Infof("New node joined: %s", nodeID)
	}

	// 更新状态
	member.Status = status
	member.UpdateLastSeen()
	member.SetHealthy(status.IsHealthy)

	return nil
}

// runHealthChecker 运行健康检查器
func (ln *LeaderNode) runHealthChecker() {
	defer ln.wg.Done()

	ticker := time.NewTicker(ln.config.HeartbeatTimeout / 3) // 每10秒检查一次
	defer ticker.Stop()

	for {
		select {
		case <-ln.ctx.Done():
			return
		case <-ticker.C:
			ln.checkNodeHealth()
		}
	}
}

// runGlobalStateUpdater 运行全局状态更新器
func (ln *LeaderNode) runGlobalStateUpdater() {
	defer ln.wg.Done()

	ticker := time.NewTicker(ln.config.GlobalSyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ln.ctx.Done():
			return
		case <-ticker.C:
			ln.updateGlobalState()
		}
	}
}

// runLeaderSynchronizer 运行组长间同步器
func (ln *LeaderNode) runLeaderSynchronizer() {
	defer ln.wg.Done()

	ticker := time.NewTicker(ln.config.LeaderSyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ln.ctx.Done():
			return
		case <-ticker.C:
			ln.synchronizeWithOtherLeaders()
		}
	}
}

// checkNodeHealth 检查节点健康状态
func (ln *LeaderNode) checkNodeHealth() {
	ln.mu.Lock()
	defer ln.mu.Unlock()

	expiredNodes := make([]string, 0)

	for nodeID, member := range ln.members {
		if member.IsExpired(ln.config.HeartbeatTimeout) {
			member.SetHealthy(false)
			expiredNodes = append(expiredNodes, nodeID)
			log.Warnf("Node %s marked as unhealthy (last seen: %v)", nodeID, member.GetLastSeen())
		}
	}

	// 移除长时间失联的节点
	for _, nodeID := range expiredNodes {
		if time.Since(ln.members[nodeID].GetLastSeen()) > ln.config.HeartbeatTimeout*2 {
			delete(ln.members, nodeID)
			log.Infof("Node %s removed from member list", nodeID)
		}
	}
}

// getGlobalState 获取全局状态
func (ln *LeaderNode) getGlobalState() *GlobalNetworkState {
	ln.stateMu.RLock()
	defer ln.stateMu.RUnlock()

	// 创建副本以避免并发访问问题
	state := &GlobalNetworkState{
		Timestamp:     ln.globalState.Timestamp,
		LocalRegion:   ln.globalState.LocalRegion,
		LocalNodes:    make(map[string]NodeStatus),
		RemoteRegions: make(map[string]RegionSummary),
		TotalNodes:    ln.globalState.TotalNodes,
		TotalRegions:  ln.globalState.TotalRegions,
	}

	// 复制本地节点
	for nodeID, status := range ln.globalState.LocalNodes {
		state.LocalNodes[nodeID] = status
	}

	// 复制远程区域
	for regionID, summary := range ln.globalState.RemoteRegions {
		state.RemoteRegions[regionID] = summary
	}

	return state
}

// generateRegionSummary 生成区域摘要
func (ln *LeaderNode) generateRegionSummary() RegionSummary {
	ln.mu.RLock()
	defer ln.mu.RUnlock()

	totalNodes := len(ln.members)
	healthyNodes := 0
	totalCPU := 0.0
	totalMemory := 0.0
	totalBandwidth := int64(0)
	virtualQueueSum := 0.0
	virtualQueueCount := 0

	// 计算统计信息
	for _, member := range ln.members {
		if member.IsHealthy {
			healthyNodes++
			totalCPU += member.Status.CPUUsage
			totalMemory += member.Status.MemoryUsage
			totalBandwidth += member.Status.OutboundBandwidth

			// 计算虚拟队列平均值
			for _, queueValue := range member.Status.VirtualQueues {
				virtualQueueSum += queueValue
				virtualQueueCount++
			}
		}
	}

	// 计算平均值
	avgCPU := 0.0
	avgMemory := 0.0
	avgVirtualQueue := 0.0

	if healthyNodes > 0 {
		avgCPU = totalCPU / float64(healthyNodes)
		avgMemory = totalMemory / float64(healthyNodes)
	}

	if virtualQueueCount > 0 {
		avgVirtualQueue = virtualQueueSum / float64(virtualQueueCount)
	}

	return RegionSummary{
		RegionID:            ln.config.Region,
		Timestamp:           time.Now(),
		TotalNodes:          totalNodes,
		HealthyNodes:        healthyNodes,
		AverageCPUUsage:     avgCPU,
		AverageMemoryUsage:  avgMemory,
		TotalBandwidth:      totalBandwidth,
		AverageVirtualQueue: avgVirtualQueue,
		InterRegionLinks:    make(map[string]LinkQuality), // TODO: 实现链路质量计算
		VirtualQueueSummary: make(map[string]float64),     // TODO: 实现详细队列摘要
	}
}

// getRegionNodes 获取区域节点列表
func (ln *LeaderNode) getRegionNodes() map[string]NodeMember {
	ln.mu.RLock()
	defer ln.mu.RUnlock()

	nodes := make(map[string]NodeMember)
	for nodeID, member := range ln.members {
		// 创建副本以避免并发访问问题
		nodes[nodeID] = NodeMember{
			NodeID:    member.NodeID,
			Address:   member.Address,
			Region:    member.Region,
			IsHealthy: member.IsHealthy,
			LastSeen:  member.GetLastSeen(),
			JoinedAt:  member.JoinedAt,
			Status:    member.Status,
		}
	}

	return nodes
}

// updateGlobalState 更新全局状态
func (ln *LeaderNode) updateGlobalState() {
	ln.stateMu.Lock()
	defer ln.stateMu.Unlock()

	// 更新本地节点状态
	ln.mu.RLock()
	localNodes := make(map[string]NodeStatus)
	for nodeID, member := range ln.members {
		if member.IsHealthy {
			localNodes[nodeID] = member.Status
		}
	}
	ln.mu.RUnlock()

	// 更新远程区域信息
	ln.regionMu.RLock()
	remoteRegions := make(map[string]RegionSummary)
	for regionID, summary := range ln.remoteRegions {
		remoteRegions[regionID] = summary
	}
	ln.regionMu.RUnlock()

	// 更新全局状态
	ln.globalState.Timestamp = time.Now()
	ln.globalState.LocalNodes = localNodes
	ln.globalState.RemoteRegions = remoteRegions
	ln.globalState.TotalNodes = len(localNodes)
	ln.globalState.TotalRegions = len(remoteRegions) + 1 // +1 for local region

	log.Debugf("Global state updated: %d local nodes, %d remote regions",
		len(localNodes), len(remoteRegions))
}

// synchronizeWithOtherLeaders 与其他组长同步
func (ln *LeaderNode) synchronizeWithOtherLeaders() {
	for _, leaderAddr := range ln.config.LeaderNodes {
		go ln.syncWithLeader(leaderAddr)
	}
}

// syncWithLeader 与指定组长同步
func (ln *LeaderNode) syncWithLeader(leaderAddr string) {
	url := fmt.Sprintf("http://%s/api/v1/region-summary", leaderAddr)

	client := &http.Client{Timeout: ln.config.RequestTimeout}
	resp, err := client.Get(url)
	if err != nil {
		log.Debugf("Failed to sync with leader %s: %v", leaderAddr, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Debugf("Leader sync failed with status %d from %s", resp.StatusCode, leaderAddr)
		return
	}

	var response RegionSummaryResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		log.Debugf("Failed to decode response from leader %s: %v", leaderAddr, err)
		return
	}

	if response.Success {
		ln.updateRemoteRegion(response.Summary)
		log.Debugf("Successfully synced with leader %s for region %s",
			leaderAddr, response.Summary.RegionID)
	}
}

// updateRemoteRegion 更新远程区域信息
func (ln *LeaderNode) updateRemoteRegion(summary RegionSummary) {
	ln.regionMu.Lock()
	defer ln.regionMu.Unlock()
	ln.remoteRegions[summary.RegionID] = summary
}

// extractNodeIDFromPath 从URL路径提取节点ID
func extractNodeIDFromPath(path string) string {
	// 路径格式: /api/v1/nodes/{node_id}/status
	// 使用字符串分割的方式解析
	if len(path) < 15 {
		return ""
	}

	// 移除前缀 "/api/v1/nodes/"
	if !strings.HasPrefix(path, "/api/v1/nodes/") {
		return ""
	}

	remaining := path[14:] // 去掉 "/api/v1/nodes/"

	// 查找 "/status" 后缀
	if !strings.HasSuffix(remaining, "/status") {
		return ""
	}

	// 提取节点ID
	nodeID := remaining[:len(remaining)-7] // 去掉 "/status"
	return nodeID
}

// handleRoutingOptimize 处理路由优化请求
func (ln *LeaderNode) handleRoutingOptimize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 解析请求
	var request RoutingRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// 验证请求参数
	if request.SourceID == "" || request.DestinationID == "" {
		http.Error(w, "Source and destination IDs are required", http.StatusBadRequest)
		return
	}

	// 设置默认值
	if request.Priority <= 0 {
		request.Priority = 1.0
	}
	if request.FairnessAlpha < 0 {
		request.FairnessAlpha = ln.trafficScheduler.config.DefaultAlpha
	}
	if request.Timestamp == 0 {
		request.Timestamp = time.Now().Unix()
	}

	log.Debugf("Received routing request: %+v", request)

	// 更新调度器的全局状态
	ln.stateMu.RLock()
	globalState := ln.globalState
	ln.stateMu.RUnlock()

	if globalState != nil {
		ln.trafficScheduler.UpdateGlobalState(globalState)
	}

	// 计算最优路由
	response, err := ln.trafficScheduler.ComputeOptimalRoute(&request)
	if err != nil {
		log.Errorf("Failed to compute optimal route: %v", err)
		http.Error(w, fmt.Sprintf("Failed to compute route: %v", err), http.StatusInternalServerError)
		return
	}

	log.Infof("Computed optimal route for task %s: path=%v, rate=%.2f",
		request.TaskID, response.OptimalPath, response.RecommendedRate)

	// 返回响应
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Errorf("Failed to encode response: %v", err)
	}
}
