package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rigel/pkg/log"
)

// DecisionMaker 决策制定器 - 负责向代理节点请求路径计算
type DecisionMaker struct {
	// 配置
	proxyNodes     []string
	requestTimeout time.Duration
	retryAttempts  int

	// 节点选择策略
	nodeSelector *NodeSelector

	// HTTP客户端
	httpClient *http.Client

	// 统计
	totalRequests   int64
	successRequests int64
	failedRequests  int64
	totalLatency    int64 // microseconds

	// 控制
	mu sync.RWMutex
}

// DecisionMakerStats 决策制定器统计
type DecisionMakerStats struct {
	TotalRequests   int64   `json:"total_requests"`
	SuccessRequests int64   `json:"success_requests"`
	FailedRequests  int64   `json:"failed_requests"`
	SuccessRate     float64 `json:"success_rate"`
	AverageLatency  float64 `json:"average_latency_ms"`
}

// NewDecisionMaker 创建决策制定器
func NewDecisionMaker(proxyNodes []string, requestTimeout time.Duration, retryAttempts int) (*DecisionMaker, error) {
	if len(proxyNodes) == 0 {
		return nil, fmt.Errorf("no proxy nodes provided")
	}

	dm := &DecisionMaker{
		proxyNodes:     proxyNodes,
		requestTimeout: requestTimeout,
		retryAttempts:  retryAttempts,
		httpClient: &http.Client{
			Timeout: requestTimeout,
		},
	}

	// 初始化节点选择器
	nodeSelector, err := NewNodeSelector(proxyNodes)
	if err != nil {
		return nil, fmt.Errorf("failed to create node selector: %v", err)
	}
	dm.nodeSelector = nodeSelector

	log.Infof("Decision maker created: %d proxy nodes, timeout=%v, retries=%d",
		len(proxyNodes), requestTimeout, retryAttempts)

	return dm, nil
}

// RequestPath 向代理节点请求路径计算
func (dm *DecisionMaker) RequestPath(request *PathRequest) (*PathResponse, error) {
	atomic.AddInt64(&dm.totalRequests, 1)
	startTime := time.Now()

	var lastErr error

	// 重试机制
	for attempt := 0; attempt <= dm.retryAttempts; attempt++ {
		// 选择代理节点
		selectedNode, err := dm.nodeSelector.SelectNode()
		if err != nil {
			lastErr = fmt.Errorf("failed to select node: %v", err)
			continue
		}

		log.Debugf("Requesting path from node %s (attempt %d/%d)",
			selectedNode, attempt+1, dm.retryAttempts+1)

		// 发送请求
		response, err := dm.sendPathRequest(selectedNode, request)
		if err != nil {
			lastErr = err
			dm.nodeSelector.ReportFailure(selectedNode)
			log.Debugf("Path request failed to %s: %v", selectedNode, err)
			continue
		}

		// 成功
		dm.nodeSelector.ReportSuccess(selectedNode)
		atomic.AddInt64(&dm.successRequests, 1)

		latency := time.Since(startTime).Microseconds()
		atomic.AddInt64(&dm.totalLatency, latency)

		log.Debugf("Path request successful from %s: %v (latency=%.2fms)",
			selectedNode, response.Path, float64(latency)/1000)

		return response, nil
	}

	// 所有重试都失败了
	atomic.AddInt64(&dm.failedRequests, 1)
	return nil, fmt.Errorf("all path requests failed, last error: %v", lastErr)
}

// sendPathRequest 发送路径请求到指定节点
func (dm *DecisionMaker) sendPathRequest(nodeAddress string, request *PathRequest) (*PathResponse, error) {
	// 构建请求URL
	url := fmt.Sprintf("http://%s/api/v1/path/compute", nodeAddress)

	// 序列化请求
	requestBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	// 发送HTTP请求
	resp, err := dm.httpClient.Post(url, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	// 检查状态码
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP request failed with status: %d", resp.StatusCode)
	}

	// 解析响应
	var pathResponse PathResponse
	if err := json.NewDecoder(resp.Body).Decode(&pathResponse); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}

	// 验证响应
	if err := dm.validatePathResponse(&pathResponse); err != nil {
		return nil, fmt.Errorf("invalid path response: %v", err)
	}

	return &pathResponse, nil
}

// validatePathResponse 验证路径响应
func (dm *DecisionMaker) validatePathResponse(response *PathResponse) error {
	if len(response.Path) == 0 {
		return fmt.Errorf("empty path")
	}

	if response.RecommendedRate <= 0 {
		return fmt.Errorf("invalid recommended rate: %.2f", response.RecommendedRate)
	}

	if response.TTL <= 0 {
		return fmt.Errorf("invalid TTL: %d", response.TTL)
	}

	return nil
}

// GetStats 获取统计信息
func (dm *DecisionMaker) GetStats() *DecisionMakerStats {
	totalRequests := atomic.LoadInt64(&dm.totalRequests)
	successRequests := atomic.LoadInt64(&dm.successRequests)
	totalLatency := atomic.LoadInt64(&dm.totalLatency)

	var successRate float64
	if totalRequests > 0 {
		successRate = float64(successRequests) / float64(totalRequests)
	}

	var averageLatency float64
	if successRequests > 0 {
		averageLatency = float64(totalLatency) / float64(successRequests) / 1000 // convert to ms
	}

	return &DecisionMakerStats{
		TotalRequests:   totalRequests,
		SuccessRequests: successRequests,
		FailedRequests:  atomic.LoadInt64(&dm.failedRequests),
		SuccessRate:     successRate,
		AverageLatency:  averageLatency,
	}
}

// Close 关闭决策制定器
func (dm *DecisionMaker) Close() error {
	log.Info("Decision maker closing")

	if dm.nodeSelector != nil {
		dm.nodeSelector.Close()
	}

	log.Info("Decision maker closed")
	return nil
}

// NodeSelector 节点选择器
type NodeSelector struct {
	nodes     []string
	nodeStats map[string]*NodeStats
	strategy  SelectionStrategy
	mu        sync.RWMutex
}

// NodeStats 节点统计
type NodeStats struct {
	TotalRequests   int64
	SuccessRequests int64
	FailedRequests  int64
	LastSuccess     time.Time
	LastFailure     time.Time
	AverageLatency  float64
}

// SelectionStrategy 选择策略
type SelectionStrategy int

const (
	StrategyRoundRobin SelectionStrategy = iota
	StrategyRandom
	StrategyLeastLoaded
	StrategyFastest
)

// NewNodeSelector 创建节点选择器
func NewNodeSelector(nodes []string) (*NodeSelector, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no nodes provided")
	}

	ns := &NodeSelector{
		nodes:     nodes,
		nodeStats: make(map[string]*NodeStats),
		strategy:  StrategyRandom, // 默认使用随机策略
	}

	// 初始化节点统计
	for _, node := range nodes {
		ns.nodeStats[node] = &NodeStats{}
	}

	return ns, nil
}

// SelectNode 选择节点
func (ns *NodeSelector) SelectNode() (string, error) {
	ns.mu.RLock()
	defer ns.mu.RUnlock()

	if len(ns.nodes) == 0 {
		return "", fmt.Errorf("no available nodes")
	}

	switch ns.strategy {
	case StrategyRandom:
		return ns.selectRandom(), nil
	case StrategyLeastLoaded:
		return ns.selectLeastLoaded(), nil
	case StrategyFastest:
		return ns.selectFastest(), nil
	default:
		return ns.selectRandom(), nil
	}
}

// selectRandom 随机选择
func (ns *NodeSelector) selectRandom() string {
	index := rand.Intn(len(ns.nodes))
	return ns.nodes[index]
}

// selectLeastLoaded 选择负载最轻的节点
func (ns *NodeSelector) selectLeastLoaded() string {
	var bestNode string
	var minLoad int64 = -1

	for _, node := range ns.nodes {
		stats := ns.nodeStats[node]
		load := stats.TotalRequests - stats.SuccessRequests

		if minLoad == -1 || load < minLoad {
			minLoad = load
			bestNode = node
		}
	}

	if bestNode == "" {
		return ns.selectRandom()
	}

	return bestNode
}

// selectFastest 选择最快的节点
func (ns *NodeSelector) selectFastest() string {
	var bestNode string
	var minLatency float64 = -1

	for _, node := range ns.nodes {
		stats := ns.nodeStats[node]

		if stats.SuccessRequests == 0 {
			continue // 跳过没有成功记录的节点
		}

		if minLatency == -1 || stats.AverageLatency < minLatency {
			minLatency = stats.AverageLatency
			bestNode = node
		}
	}

	if bestNode == "" {
		return ns.selectRandom()
	}

	return bestNode
}

// ReportSuccess 报告成功
func (ns *NodeSelector) ReportSuccess(node string) {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	if stats, exists := ns.nodeStats[node]; exists {
		stats.TotalRequests++
		stats.SuccessRequests++
		stats.LastSuccess = time.Now()
	}
}

// ReportFailure 报告失败
func (ns *NodeSelector) ReportFailure(node string) {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	if stats, exists := ns.nodeStats[node]; exists {
		stats.TotalRequests++
		stats.FailedRequests++
		stats.LastFailure = time.Now()
	}
}

// Close 关闭节点选择器
func (ns *NodeSelector) Close() error {
	return nil
}
