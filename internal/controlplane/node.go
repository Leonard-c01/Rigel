package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/rigel/pkg/log"
)

// RegularNode 普通节点实现
type RegularNode struct {
	config *ControlPlaneConfig

	// 本地状态
	localStatus NodeStatus
	statusMu    sync.RWMutex

	// 全局状态缓存
	globalState *GlobalNetworkState
	stateMu     sync.RWMutex

	// HTTP客户端
	httpClient *http.Client

	// UMW路由调度器
	trafficScheduler *TrafficScheduler

	// HTTP服务器 (用于提供路由优化服务)
	httpServer *http.Server

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewRegularNode 创建普通节点
func NewRegularNode(config *ControlPlaneConfig) *RegularNode {
	if config == nil {
		config = DefaultControlPlaneConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &RegularNode{
		config: config,
		localStatus: NodeStatus{
			NodeID:        config.NodeID,
			Region:        config.Region,
			Address:       config.Address,
			VirtualQueues: make(map[string]float64),
			IsHealthy:     true,
		},
		httpClient: &http.Client{
			Timeout: config.RequestTimeout,
		},
		trafficScheduler: NewTrafficScheduler(DefaultUMWConfig()),
		ctx:              ctx,
		cancel:           cancel,
	}
}

// Start 启动普通节点
func (rn *RegularNode) Start() error {
	log.Infof("Starting regular node: %s in region: %s", rn.config.NodeID, rn.config.Region)

	// 启动HTTP API服务器 (用于提供路由优化服务)
	if err := rn.startAPIServer(); err != nil {
		return fmt.Errorf("failed to start API server: %v", err)
	}

	// 启动后台任务
	rn.wg.Add(3)
	go rn.runStatusReporter()
	go rn.runGlobalStateFetcher()
	go rn.runTrafficSchedulerUpdater()

	log.Infof("Regular node started successfully")
	return nil
}

// Stop 停止普通节点
func (rn *RegularNode) Stop() error {
	log.Infof("Stopping regular node...")

	rn.cancel()

	// 停止HTTP服务器
	if rn.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rn.httpServer.Shutdown(ctx)
	}

	rn.wg.Wait()

	log.Infof("Regular node stopped")
	return nil
}

// runStatusReporter 运行状态上报器
func (rn *RegularNode) runStatusReporter() {
	defer rn.wg.Done()

	ticker := time.NewTicker(rn.config.StatusReportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rn.ctx.Done():
			return
		case <-ticker.C:
			if err := rn.reportStatus(); err != nil {
				log.Errorf("Failed to report status: %v", err)
			}
		}
	}
}

// runGlobalStateFetcher 运行全局状态获取器
func (rn *RegularNode) runGlobalStateFetcher() {
	defer rn.wg.Done()

	ticker := time.NewTicker(rn.config.GlobalSyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rn.ctx.Done():
			return
		case <-ticker.C:
			if err := rn.fetchGlobalState(); err != nil {
				log.Debugf("Failed to fetch global state: %v", err)
			}
		}
	}
}

// reportStatus 上报状态到组长节点
func (rn *RegularNode) reportStatus() error {
	// 更新本地状态
	rn.updateLocalStatus()

	// 构建请求
	request := StatusReportRequest{
		NodeStatus: rn.getLocalStatus(),
	}

	requestBody, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %v", err)
	}

	// 发送请求到组长节点
	url := fmt.Sprintf("http://%s/api/v1/nodes/%s/status",
		rn.config.LeaderAddress, rn.config.NodeID)

	resp, err := rn.httpClient.Post(url, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return fmt.Errorf("failed to send status report: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status report failed with status: %d", resp.StatusCode)
	}

	var response StatusReportResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return fmt.Errorf("failed to decode response: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("status report rejected: %s", response.Message)
	}

	log.Debugf("Status reported successfully to leader")
	return nil
}

// fetchGlobalState 从组长节点获取全局状态
func (rn *RegularNode) fetchGlobalState() error {
	url := fmt.Sprintf("http://%s/api/v1/global-status", rn.config.LeaderAddress)

	resp, err := rn.httpClient.Get(url)
	if err != nil {
		return fmt.Errorf("failed to fetch global state: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("global state fetch failed with status: %d", resp.StatusCode)
	}

	var response GlobalStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return fmt.Errorf("failed to decode response: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("global state fetch rejected: %s", response.Message)
	}

	// 更新本地缓存的全局状态
	rn.updateGlobalState(&response.GlobalState)

	log.Debugf("Global state fetched successfully")
	return nil
}

// updateLocalStatus 更新本地状态
func (rn *RegularNode) updateLocalStatus() {
	rn.statusMu.Lock()
	defer rn.statusMu.Unlock()

	// 更新时间戳
	rn.localStatus.Timestamp = time.Now()

	// 获取系统资源使用情况
	rn.localStatus.CPUUsage = rn.getCPUUsage()
	rn.localStatus.MemoryUsage = rn.getMemoryUsage()

	// 获取网络带宽信息 (简化实现)
	rn.localStatus.InboundBandwidth = rn.getInboundBandwidth()
	rn.localStatus.OutboundBandwidth = rn.getOutboundBandwidth()

	// 更新虚拟队列状态 (这里需要与路由算法集成)
	rn.updateVirtualQueues()

	// 设置健康状态
	rn.localStatus.IsHealthy = true
}

// getCPUUsage 获取CPU使用率 (简化实现)
func (rn *RegularNode) getCPUUsage() float64 {
	// 实际实现应该使用系统调用获取真实的CPU使用率
	// 这里返回一个模拟值
	return 0.1 + (float64(time.Now().UnixNano()%100) / 1000.0)
}

// getMemoryUsage 获取内存使用率
func (rn *RegularNode) getMemoryUsage() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// 简化计算：已分配内存 / 系统内存 (这里假设系统有8GB内存)
	systemMemory := uint64(8 * 1024 * 1024 * 1024) // 8GB
	return float64(m.Alloc) / float64(systemMemory)
}

// getInboundBandwidth 获取入站带宽 (简化实现)
func (rn *RegularNode) getInboundBandwidth() int64 {
	// 实际实现应该从网络接口统计信息获取
	return 1024 * 1024 * 10 // 10MB/s
}

// getOutboundBandwidth 获取出站带宽 (简化实现)
func (rn *RegularNode) getOutboundBandwidth() int64 {
	// 实际实现应该从网络接口统计信息获取
	return 1024 * 1024 * 10 // 10MB/s
}

// updateVirtualQueues 更新虚拟队列状态
func (rn *RegularNode) updateVirtualQueues() {
	// 这里需要与Lyapunov路由算法集成
	// 目前使用模拟数据
	rn.localStatus.VirtualQueues["link_1"] = 0.5 + (float64(time.Now().UnixNano()%100) / 200.0)
	rn.localStatus.VirtualQueues["link_2"] = 0.3 + (float64(time.Now().UnixNano()%150) / 300.0)
}

// getLocalStatus 获取本地状态
func (rn *RegularNode) getLocalStatus() NodeStatus {
	rn.statusMu.RLock()
	defer rn.statusMu.RUnlock()

	// 创建副本
	status := rn.localStatus
	status.VirtualQueues = make(map[string]float64)
	for k, v := range rn.localStatus.VirtualQueues {
		status.VirtualQueues[k] = v
	}

	return status
}

// updateGlobalState 更新全局状态缓存
func (rn *RegularNode) updateGlobalState(state *GlobalNetworkState) {
	rn.stateMu.Lock()
	defer rn.stateMu.Unlock()
	rn.globalState = state
}

// GetGlobalState 获取全局状态 (供外部调用)
func (rn *RegularNode) GetGlobalState() *GlobalNetworkState {
	rn.stateMu.RLock()
	defer rn.stateMu.RUnlock()
	return rn.globalState
}

// UpdateVirtualQueue 更新指定链路的虚拟队列值 (供路由算法调用)
func (rn *RegularNode) UpdateVirtualQueue(linkID string, value float64) {
	rn.statusMu.Lock()
	defer rn.statusMu.Unlock()
	rn.localStatus.VirtualQueues[linkID] = value
}

// startAPIServer 启动API服务器
func (rn *RegularNode) startAPIServer() error {
	// 如果没有配置API端口，则不启动服务器
	if rn.config.APIPort == "" {
		return nil
	}

	mux := http.NewServeMux()

	// 注册路由优化API端点
	mux.HandleFunc("/api/v1/routing/optimize", rn.handleRoutingOptimize)

	// 注册节点状态查询端点
	mux.HandleFunc("/api/v1/node/status", rn.handleNodeStatus)

	rn.httpServer = &http.Server{
		Addr:         rn.config.APIPort,
		Handler:      mux,
		ReadTimeout:  rn.config.RequestTimeout,
		WriteTimeout: rn.config.RequestTimeout,
	}

	go func() {
		if err := rn.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("Regular node API server error: %v", err)
		}
	}()

	log.Infof("Regular node API server started on %s", rn.config.APIPort)
	return nil
}

// runTrafficSchedulerUpdater 运行流量调度器更新器
func (rn *RegularNode) runTrafficSchedulerUpdater() {
	defer rn.wg.Done()

	ticker := time.NewTicker(rn.config.GlobalSyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rn.ctx.Done():
			return
		case <-ticker.C:
			// 更新调度器的全局状态
			rn.stateMu.RLock()
			globalState := rn.globalState
			rn.stateMu.RUnlock()

			if globalState != nil && rn.trafficScheduler != nil {
				rn.trafficScheduler.UpdateGlobalState(globalState)
			}
		}
	}
}

// handleRoutingOptimize 处理路由优化请求
func (rn *RegularNode) handleRoutingOptimize(w http.ResponseWriter, r *http.Request) {
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
		request.FairnessAlpha = rn.trafficScheduler.config.DefaultAlpha
	}
	if request.Timestamp == 0 {
		request.Timestamp = time.Now().Unix()
	}

	log.Debugf("Regular node received routing request: %+v", request)

	// 确保调度器有最新的全局状态
	rn.stateMu.RLock()
	globalState := rn.globalState
	rn.stateMu.RUnlock()

	if globalState != nil {
		rn.trafficScheduler.UpdateGlobalState(globalState)
	}

	// 计算最优路由
	response, err := rn.trafficScheduler.ComputeOptimalRoute(&request)
	if err != nil {
		log.Errorf("Failed to compute optimal route: %v", err)
		http.Error(w, fmt.Sprintf("Failed to compute route: %v", err), http.StatusInternalServerError)
		return
	}

	log.Infof("Regular node computed optimal route for task %s: path=%v, rate=%.2f",
		request.TaskID, response.OptimalPath, response.RecommendedRate)

	// 返回响应
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Errorf("Failed to encode response: %v", err)
	}
}

// handleNodeStatus 处理节点状态查询请求
func (rn *RegularNode) handleNodeStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 获取本地状态
	status := rn.getLocalStatus()

	// 获取调度器统计信息
	var schedulerStats map[string]interface{}
	if rn.trafficScheduler != nil {
		schedulerStats = rn.trafficScheduler.GetStatistics()
	}

	response := map[string]interface{}{
		"node_status":     status,
		"scheduler_stats": schedulerStats,
		"global_state_time": func() int64 {
			rn.stateMu.RLock()
			defer rn.stateMu.RUnlock()
			if rn.globalState != nil {
				return rn.globalState.Timestamp.Unix()
			}
			return 0
		}(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Errorf("Failed to encode node status response: %v", err)
	}
}
