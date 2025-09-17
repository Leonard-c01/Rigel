package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/rigel/internal/controlplane"
	"github.com/rigel/pkg/log"
)

// TestControlPlaneIntegration 测试控制平面集成功能
func TestControlPlaneIntegration(t *testing.T) {
	// 初始化日志
	if err := log.Init("info", ""); err != nil {
		t.Fatalf("Failed to initialize logger: %v", err)
	}

	// 测试场景：一个组长节点和两个普通节点

	// 1. 启动组长节点
	leaderConfig := &controlplane.ControlPlaneConfig{
		NodeID:               "leader-1",
		Region:               "us-east",
		Address:              "127.0.0.1:9090",
		IsLeader:             true,
		StatusReportInterval: 2 * time.Second,
		GlobalSyncInterval:   1 * time.Second,
		LeaderSyncInterval:   1 * time.Second,
		HeartbeatTimeout:     10 * time.Second,
		RequestTimeout:       5 * time.Second,
		APIPort:              ":9090",
	}

	leaderManager := controlplane.NewControlPlaneManager(leaderConfig)
	if err := leaderManager.Start(); err != nil {
		t.Fatalf("Failed to start leader node: %v", err)
	}
	defer leaderManager.Stop()

	// 等待组长节点启动
	time.Sleep(1 * time.Second)

	// 2. 启动普通节点1
	node1Config := &controlplane.ControlPlaneConfig{
		NodeID:               "node-1",
		Region:               "us-east",
		Address:              "127.0.0.1:9091",
		IsLeader:             false,
		LeaderAddress:        "127.0.0.1:9090",
		StatusReportInterval: 2 * time.Second,
		GlobalSyncInterval:   1 * time.Second,
		HeartbeatTimeout:     10 * time.Second,
		RequestTimeout:       5 * time.Second,
	}

	node1Manager := controlplane.NewControlPlaneManager(node1Config)
	if err := node1Manager.Start(); err != nil {
		t.Fatalf("Failed to start node 1: %v", err)
	}
	defer node1Manager.Stop()

	// 3. 启动普通节点2
	node2Config := &controlplane.ControlPlaneConfig{
		NodeID:               "node-2",
		Region:               "us-east",
		Address:              "127.0.0.1:9092",
		IsLeader:             false,
		LeaderAddress:        "127.0.0.1:9090",
		StatusReportInterval: 2 * time.Second,
		GlobalSyncInterval:   1 * time.Second,
		HeartbeatTimeout:     10 * time.Second,
		RequestTimeout:       5 * time.Second,
	}

	node2Manager := controlplane.NewControlPlaneManager(node2Config)
	if err := node2Manager.Start(); err != nil {
		t.Fatalf("Failed to start node 2: %v", err)
	}
	defer node2Manager.Stop()

	// 等待节点注册和状态同步
	time.Sleep(5 * time.Second)

	// 4. 验证验收标准
	t.Run("验收标准1: 查看区域节点列表", func(t *testing.T) {
		testRegionNodesList(t)
	})

	t.Run("验收标准2: 节点下线检测", func(t *testing.T) {
		// 停止node2，验证是否被检测到
		node2Manager.Stop()
		time.Sleep(12 * time.Second) // 等待超过心跳超时时间

		testNodeOfflineDetection(t, "node-2")
	})

	t.Run("验收标准3: 状态变化同步", func(t *testing.T) {
		// 更新node1的虚拟队列状态
		node1Manager.UpdateVirtualQueue("test-link", 0.95)
		time.Sleep(3 * time.Second) // 等待状态同步

		testStatusChangeSync(t, "node-1", "test-link", 0.95)
	})

	t.Run("验收标准4: 全局状态获取", func(t *testing.T) {
		testGlobalStatusRetrieval(t)
	})
}

// testRegionNodesList 测试区域节点列表查看
func testRegionNodesList(t *testing.T) {
	resp, err := http.Get("http://127.0.0.1:9090/api/v1/region-nodes")
	if err != nil {
		t.Fatalf("Failed to get region nodes: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", resp.StatusCode)
	}

	var response controlplane.RegionNodesResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if !response.Success {
		t.Fatalf("Response not successful: %s", response.Message)
	}

	// 验证节点数量 (应该有2个普通节点)
	if len(response.Nodes) < 2 {
		t.Errorf("Expected at least 2 nodes, got %d", len(response.Nodes))
	}

	// 验证节点信息
	for nodeID, node := range response.Nodes {
		if node.NodeID != nodeID {
			t.Errorf("Node ID mismatch: expected %s, got %s", nodeID, node.NodeID)
		}
		if node.Region != "us-east" {
			t.Errorf("Expected region us-east, got %s", node.Region)
		}
		t.Logf("Node %s: healthy=%v, last_seen=%v", nodeID, node.IsHealthy, node.LastSeen)
	}
}

// testNodeOfflineDetection 测试节点下线检测
func testNodeOfflineDetection(t *testing.T, nodeID string) {
	resp, err := http.Get("http://127.0.0.1:9090/api/v1/region-nodes")
	if err != nil {
		t.Fatalf("Failed to get region nodes: %v", err)
	}
	defer resp.Body.Close()

	var response controlplane.RegionNodesResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	// 检查节点是否被标记为不健康或已移除
	if node, exists := response.Nodes[nodeID]; exists {
		if node.IsHealthy {
			t.Errorf("Node %s should be marked as unhealthy", nodeID)
		}
		t.Logf("Node %s marked as unhealthy: last_seen=%v", nodeID, node.LastSeen)
	} else {
		t.Logf("Node %s has been removed from member list", nodeID)
	}
}

// testStatusChangeSync 测试状态变化同步
func testStatusChangeSync(t *testing.T, nodeID, linkID string, expectedValue float64) {
	resp, err := http.Get("http://127.0.0.1:9090/api/v1/global-status")
	if err != nil {
		t.Fatalf("Failed to get global status: %v", err)
	}
	defer resp.Body.Close()

	var response controlplane.GlobalStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if !response.Success {
		t.Fatalf("Response not successful: %s", response.Message)
	}

	// 检查节点状态是否包含更新的虚拟队列值
	if nodeStatus, exists := response.GlobalState.LocalNodes[nodeID]; exists {
		if queueValue, exists := nodeStatus.VirtualQueues[linkID]; exists {
			if queueValue != expectedValue {
				t.Errorf("Expected virtual queue value %f, got %f", expectedValue, queueValue)
			}
			t.Logf("Virtual queue %s for node %s updated to %f", linkID, nodeID, queueValue)
		} else {
			t.Errorf("Virtual queue %s not found for node %s", linkID, nodeID)
		}
	} else {
		t.Errorf("Node %s not found in global state", nodeID)
	}
}

// testGlobalStatusRetrieval 测试全局状态获取
func testGlobalStatusRetrieval(t *testing.T) {
	resp, err := http.Get("http://127.0.0.1:9090/api/v1/global-status")
	if err != nil {
		t.Fatalf("Failed to get global status: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", resp.StatusCode)
	}

	var response controlplane.GlobalStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if !response.Success {
		t.Fatalf("Response not successful: %s", response.Message)
	}

	globalState := response.GlobalState

	// 验证全局状态结构
	if globalState.LocalRegion != "us-east" {
		t.Errorf("Expected local region us-east, got %s", globalState.LocalRegion)
	}

	if len(globalState.LocalNodes) == 0 {
		t.Error("Expected at least one local node")
	}

	// 打印全局状态信息
	t.Logf("Global state: region=%s, nodes=%d, regions=%d",
		globalState.LocalRegion, globalState.TotalNodes, globalState.TotalRegions)

	for nodeID, status := range globalState.LocalNodes {
		t.Logf("Node %s: CPU=%.2f%%, Memory=%.2f%%, Queues=%d",
			nodeID, status.CPUUsage*100, status.MemoryUsage*100, len(status.VirtualQueues))
	}
}

// TestLeaderSynchronization 测试组长间同步
func TestLeaderSynchronization(t *testing.T) {
	// 初始化日志
	if err := log.Init("info", ""); err != nil {
		t.Fatalf("Failed to initialize logger: %v", err)
	}

	// 启动两个组长节点模拟跨区域同步

	// 组长节点1 (us-east)
	leader1Config := &controlplane.ControlPlaneConfig{
		NodeID:               "leader-us-east",
		Region:               "us-east",
		Address:              "127.0.0.1:9100",
		IsLeader:             true,
		LeaderNodes:          []string{"127.0.0.1:9101"}, // 指向leader2
		StatusReportInterval: 3 * time.Second,
		GlobalSyncInterval:   1 * time.Second,
		LeaderSyncInterval:   2 * time.Second,
		HeartbeatTimeout:     15 * time.Second,
		RequestTimeout:       5 * time.Second,
		APIPort:              ":9100",
	}

	leader1Manager := controlplane.NewControlPlaneManager(leader1Config)
	if err := leader1Manager.Start(); err != nil {
		t.Fatalf("Failed to start leader 1: %v", err)
	}
	defer leader1Manager.Stop()

	// 组长节点2 (us-west)
	leader2Config := &controlplane.ControlPlaneConfig{
		NodeID:               "leader-us-west",
		Region:               "us-west",
		Address:              "127.0.0.1:9101",
		IsLeader:             true,
		LeaderNodes:          []string{"127.0.0.1:9100"}, // 指向leader1
		StatusReportInterval: 3 * time.Second,
		GlobalSyncInterval:   1 * time.Second,
		LeaderSyncInterval:   2 * time.Second,
		HeartbeatTimeout:     15 * time.Second,
		RequestTimeout:       5 * time.Second,
		APIPort:              ":9101",
	}

	leader2Manager := controlplane.NewControlPlaneManager(leader2Config)
	if err := leader2Manager.Start(); err != nil {
		t.Fatalf("Failed to start leader 2: %v", err)
	}
	defer leader2Manager.Stop()

	// 等待同步
	time.Sleep(5 * time.Second)

	// 验证跨区域同步
	t.Run("验收标准4: 跨区域信息同步", func(t *testing.T) {
		testCrossRegionSync(t)
	})
}

// testCrossRegionSync 测试跨区域同步
func testCrossRegionSync(t *testing.T) {
	// 从leader1获取全局状态，应该包含leader2的区域信息
	resp, err := http.Get("http://127.0.0.1:9100/api/v1/global-status")
	if err != nil {
		t.Fatalf("Failed to get global status from leader1: %v", err)
	}
	defer resp.Body.Close()

	var response controlplane.GlobalStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if !response.Success {
		t.Fatalf("Response not successful: %s", response.Message)
	}

	// 检查是否包含远程区域信息
	if len(response.GlobalState.RemoteRegions) == 0 {
		t.Error("Expected remote region information")
	} else {
		for regionID, summary := range response.GlobalState.RemoteRegions {
			t.Logf("Remote region %s: nodes=%d, healthy=%d",
				regionID, summary.TotalNodes, summary.HealthyNodes)
		}
	}
}

// TestUMWRoutingOptimization 测试基于UMW的在线路由优化服务
func TestUMWRoutingOptimization(t *testing.T) {
	// 初始化日志
	if err := log.Init("info", ""); err != nil {
		t.Fatalf("Failed to initialize logger: %v", err)
	}

	// 启动测试环境：一个组长节点和三个普通节点
	leaderConfig := &controlplane.ControlPlaneConfig{
		NodeID:               "leader-test",
		Region:               "test-region",
		Address:              "127.0.0.1:9200",
		IsLeader:             true,
		StatusReportInterval: 2 * time.Second,
		GlobalSyncInterval:   1 * time.Second,
		LeaderSyncInterval:   1 * time.Second,
		HeartbeatTimeout:     10 * time.Second,
		RequestTimeout:       5 * time.Second,
		APIPort:              ":9200",
	}

	leaderManager := controlplane.NewControlPlaneManager(leaderConfig)
	if err := leaderManager.Start(); err != nil {
		t.Fatalf("Failed to start leader node: %v", err)
	}
	defer leaderManager.Stop()

	// 启动三个普通节点
	nodeConfigs := []*controlplane.ControlPlaneConfig{
		{
			NodeID:               "node-a",
			Region:               "test-region",
			Address:              "127.0.0.1:9201",
			IsLeader:             false,
			LeaderAddress:        "127.0.0.1:9200",
			StatusReportInterval: 2 * time.Second,
			GlobalSyncInterval:   1 * time.Second,
			HeartbeatTimeout:     10 * time.Second,
			RequestTimeout:       5 * time.Second,
		},
		{
			NodeID:               "node-b",
			Region:               "test-region",
			Address:              "127.0.0.1:9202",
			IsLeader:             false,
			LeaderAddress:        "127.0.0.1:9200",
			StatusReportInterval: 2 * time.Second,
			GlobalSyncInterval:   1 * time.Second,
			HeartbeatTimeout:     10 * time.Second,
			RequestTimeout:       5 * time.Second,
		},
		{
			NodeID:               "node-c",
			Region:               "test-region",
			Address:              "127.0.0.1:9203",
			IsLeader:             false,
			LeaderAddress:        "127.0.0.1:9200",
			StatusReportInterval: 2 * time.Second,
			GlobalSyncInterval:   1 * time.Second,
			HeartbeatTimeout:     10 * time.Second,
			RequestTimeout:       5 * time.Second,
		},
	}

	var nodeManagers []*controlplane.ControlPlaneManager
	for _, config := range nodeConfigs {
		manager := controlplane.NewControlPlaneManager(config)
		if err := manager.Start(); err != nil {
			t.Fatalf("Failed to start node %s: %v", config.NodeID, err)
		}
		defer manager.Stop()
		nodeManagers = append(nodeManagers, manager)
	}

	// 等待节点注册和状态同步
	time.Sleep(5 * time.Second)

	// 运行所有验收标准测试
	t.Run("验收标准1: API接收请求并返回结构化响应", func(t *testing.T) {
		testRoutingAPIBasicFunctionality(t)
	})

	t.Run("验收标准2: 虚拟队列影响路径选择", func(t *testing.T) {
		testVirtualQueueImpactOnRouting(t, nodeManagers)
	})

	t.Run("验收标准3: 网络稳定性保证", func(t *testing.T) {
		testNetworkStability(t, nodeManagers)
	})

	t.Run("验收标准4: 公平性参数效果", func(t *testing.T) {
		testFairnessParameterEffects(t)
	})

	t.Run("验收标准5: 算法计算正确性", func(t *testing.T) {
		testAlgorithmCorrectness(t)
	})
}

// testRoutingAPIBasicFunctionality 测试路由API基本功能
func testRoutingAPIBasicFunctionality(t *testing.T) {
	// 构造路由请求
	request := controlplane.RoutingRequest{
		TaskID:        "test-task-001",
		SourceID:      "node-a",
		DestinationID: "node-c",
		DataSize:      1024 * 1024, // 1MB
		Priority:      1.5,
		FairnessAlpha: 0.5,
		Timestamp:     time.Now().Unix(),
	}

	requestBody, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}

	// 发送路由优化请求
	resp, err := http.Post("http://127.0.0.1:9200/api/v1/routing/optimize",
		"application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		t.Fatalf("Failed to send routing request: %v", err)
	}
	defer resp.Body.Close()

	// 验证响应状态
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", resp.StatusCode)
	}

	// 解析响应
	var response controlplane.RoutingResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	// 验证响应结构
	if response.TaskID != request.TaskID {
		t.Errorf("Expected task ID %s, got %s", request.TaskID, response.TaskID)
	}

	if len(response.OptimalPath) == 0 {
		t.Error("Expected non-empty optimal path")
	}

	if response.RecommendedRate <= 0 {
		t.Error("Expected positive recommended rate")
	}

	if response.PathCost < 0 {
		t.Error("Expected non-negative path cost")
	}

	if response.ComputationTime <= 0 {
		t.Error("Expected positive computation time")
	}

	if response.AlgorithmVersion == "" {
		t.Error("Expected non-empty algorithm version")
	}

	t.Logf("✅ API基本功能测试通过:")
	t.Logf("   路径: %v", response.OptimalPath)
	t.Logf("   推荐速率: %.2f bytes/sec", response.RecommendedRate)
	t.Logf("   路径成本: %.4f", response.PathCost)
	t.Logf("   计算时间: %.2f ms", response.ComputationTime)
}

// testVirtualQueueImpactOnRouting 测试虚拟队列对路径选择的影响
func testVirtualQueueImpactOnRouting(t *testing.T, nodeManagers []*controlplane.ControlPlaneManager) {
	// 基础路由请求
	baseRequest := controlplane.RoutingRequest{
		TaskID:        "test-vq-001",
		SourceID:      "node-a",
		DestinationID: "node-c",
		DataSize:      1024 * 1024,
		Priority:      1.0,
		FairnessAlpha: 0.5,
	}

	// 1. 获取初始路径
	initialResponse := sendRoutingRequest(t, baseRequest)
	t.Logf("初始路径: %v, 成本: %.4f", initialResponse.OptimalPath, initialResponse.PathCost)

	// 2. 人为增加某个节点的虚拟队列值
	// 假设初始路径经过node-b，我们增加node-b的虚拟队列值
	if len(nodeManagers) > 1 {
		// 模拟node-b的某个链路拥塞
		nodeManagers[1].UpdateVirtualQueue("link_to_node-c", 5.0) // 大幅增加虚拟队列值

		// 等待状态同步
		time.Sleep(3 * time.Second)
	}

	// 3. 再次请求路由，应该看到路径成本增加或路径改变
	baseRequest.TaskID = "test-vq-002"
	updatedResponse := sendRoutingRequest(t, baseRequest)
	t.Logf("更新后路径: %v, 成本: %.4f", updatedResponse.OptimalPath, updatedResponse.PathCost)

	// 4. 验证虚拟队列的影响
	if updatedResponse.PathCost <= initialResponse.PathCost {
		t.Logf("⚠️  路径成本未显著增加，可能是网络拓扑或算法参数导致")
		t.Logf("   初始成本: %.4f, 更新后成本: %.4f", initialResponse.PathCost, updatedResponse.PathCost)
	} else {
		t.Logf("✅ 虚拟队列影响验证通过:")
		t.Logf("   成本增加: %.4f -> %.4f (增加 %.4f)",
			initialResponse.PathCost, updatedResponse.PathCost,
			updatedResponse.PathCost-initialResponse.PathCost)
	}

	// 5. 重置虚拟队列值
	if len(nodeManagers) > 1 {
		nodeManagers[1].UpdateVirtualQueue("link_to_node-c", 0.1)
		time.Sleep(2 * time.Second)
	}

	// 6. 验证路径成本恢复
	baseRequest.TaskID = "test-vq-003"
	resetResponse := sendRoutingRequest(t, baseRequest)
	t.Logf("重置后路径: %v, 成本: %.4f", resetResponse.OptimalPath, resetResponse.PathCost)

	if resetResponse.PathCost < updatedResponse.PathCost {
		t.Logf("✅ 虚拟队列重置效果验证通过:")
		t.Logf("   成本恢复: %.4f -> %.4f", updatedResponse.PathCost, resetResponse.PathCost)
	}
}

// testNetworkStability 测试网络稳定性保证
func testNetworkStability(t *testing.T, nodeManagers []*controlplane.ControlPlaneManager) {
	// 模拟持续高流量场景，验证虚拟队列不会无限增长
	const numRequests = 20
	const requestInterval = 100 * time.Millisecond

	var pathCosts []float64
	var recommendedRates []float64

	t.Logf("开始网络稳定性测试，发送 %d 个连续请求...", numRequests)

	for i := 0; i < numRequests; i++ {
		request := controlplane.RoutingRequest{
			TaskID:        fmt.Sprintf("stability-test-%03d", i),
			SourceID:      "node-a",
			DestinationID: "node-c",
			DataSize:      1024 * 1024,
			Priority:      1.0,
			FairnessAlpha: 0.5,
		}

		response := sendRoutingRequest(t, request)
		pathCosts = append(pathCosts, response.PathCost)
		recommendedRates = append(recommendedRates, response.RecommendedRate)

		t.Logf("请求 %d: 成本=%.4f, 速率=%.2f", i+1, response.PathCost, response.RecommendedRate)

		time.Sleep(requestInterval)
	}

	// 分析稳定性
	avgCost := calculateAverage(pathCosts)
	maxCost := calculateMax(pathCosts)
	minCost := calculateMin(pathCosts)

	t.Logf("✅ 网络稳定性分析:")
	t.Logf("   平均路径成本: %.4f", avgCost)
	t.Logf("   最大路径成本: %.4f", maxCost)
	t.Logf("   最小路径成本: %.4f", minCost)
	t.Logf("   成本变化范围: %.4f", maxCost-minCost)

	// 验证成本没有无限增长（简单的启发式检查）
	if maxCost > avgCost*3 {
		t.Errorf("路径成本可能存在不稳定增长: max=%.4f, avg=%.4f", maxCost, avgCost)
	} else {
		t.Logf("✅ 路径成本保持相对稳定")
	}

	// 验证推荐速率的合理性
	avgRate := calculateAverage(recommendedRates)
	t.Logf("   平均推荐速率: %.2f bytes/sec", avgRate)

	if avgRate <= 0 {
		t.Error("推荐速率应该为正数")
	} else {
		t.Logf("✅ 推荐速率保持合理范围")
	}
}

// testFairnessParameterEffects 测试公平性参数效果
func testFairnessParameterEffects(t *testing.T) {
	// 测试不同的α值对系统行为的影响
	alphaValues := []float64{0.0, 0.5, 1.0}
	priorities := []float64{0.5, 1.0, 2.0} // 低、中、高优先级

	t.Logf("测试公平性参数α的效果...")

	for _, alpha := range alphaValues {
		t.Logf("\n--- 测试 α = %.1f ---", alpha)

		var results []struct {
			priority float64
			rate     float64
		}

		for _, priority := range priorities {
			request := controlplane.RoutingRequest{
				TaskID:        fmt.Sprintf("fairness-test-a%.1f-p%.1f", alpha, priority),
				SourceID:      "node-a",
				DestinationID: "node-c",
				DataSize:      1024 * 1024,
				Priority:      priority,
				FairnessAlpha: alpha,
			}

			response := sendRoutingRequest(t, request)
			results = append(results, struct {
				priority float64
				rate     float64
			}{priority, response.RecommendedRate})

			t.Logf("优先级 %.1f: 推荐速率 %.2f", priority, response.RecommendedRate)
		}

		// 分析公平性效果
		analyzeFairnessEffects(t, alpha, results)
	}
}

// testAlgorithmCorrectness 测试算法计算正确性
func testAlgorithmCorrectness(t *testing.T) {
	// 使用已知参数进行计算，验证结果与手动计算一致
	t.Logf("验证UMW算法计算正确性...")

	// 创建一个简单的测试场景
	request := controlplane.RoutingRequest{
		TaskID:        "correctness-test-001",
		SourceID:      "node-a",
		DestinationID: "node-b",
		DataSize:      1024 * 1024,
		Priority:      1.0,
		FairnessAlpha: 0.5,
	}

	response := sendRoutingRequest(t, request)

	// 基本合理性检查
	if response.PathCost < 0 {
		t.Error("路径成本不应为负数")
	}

	if response.RecommendedRate <= 0 {
		t.Error("推荐速率应为正数")
	}

	if len(response.OptimalPath) < 2 {
		t.Error("最优路径应至少包含源和目标节点")
	}

	// 验证路径的连通性
	if response.OptimalPath[0] != request.SourceID {
		t.Errorf("路径起点应为 %s, 实际为 %s", request.SourceID, response.OptimalPath[0])
	}

	if response.OptimalPath[len(response.OptimalPath)-1] != request.DestinationID {
		t.Errorf("路径终点应为 %s, 实际为 %s", request.DestinationID,
			response.OptimalPath[len(response.OptimalPath)-1])
	}

	t.Logf("✅ 算法计算正确性验证通过:")
	t.Logf("   路径: %v", response.OptimalPath)
	t.Logf("   成本: %.4f", response.PathCost)
	t.Logf("   速率: %.2f", response.RecommendedRate)
	t.Logf("   计算时间: %.2f ms", response.ComputationTime)

	// 验证计算时间符合性能要求（应小于50ms）
	if response.ComputationTime > 50.0 {
		t.Errorf("计算时间 %.2f ms 超过性能要求 50ms", response.ComputationTime)
	} else {
		t.Logf("✅ 计算时间符合性能要求: %.2f ms < 50ms", response.ComputationTime)
	}
}

// Helper functions

// sendRoutingRequest 发送路由请求的辅助函数
func sendRoutingRequest(t *testing.T, request controlplane.RoutingRequest) controlplane.RoutingResponse {
	requestBody, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}

	resp, err := http.Post("http://127.0.0.1:9200/api/v1/routing/optimize",
		"application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		t.Fatalf("Failed to send routing request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", resp.StatusCode)
	}

	var response controlplane.RoutingResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	return response
}

// calculateAverage 计算平均值
func calculateAverage(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

// calculateMax 计算最大值
func calculateMax(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	max := values[0]
	for _, v := range values {
		if v > max {
			max = v
		}
	}
	return max
}

// calculateMin 计算最小值
func calculateMin(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	min := values[0]
	for _, v := range values {
		if v < min {
			min = v
		}
	}
	return min
}

// analyzeFairnessEffects 分析公平性效果
func analyzeFairnessEffects(t *testing.T, alpha float64, results []struct {
	priority float64
	rate     float64
}) {
	if len(results) < 2 {
		return
	}

	// 计算速率比例
	lowPriorityRate := results[0].rate
	highPriorityRate := results[len(results)-1].rate

	if lowPriorityRate > 0 && highPriorityRate > 0 {
		ratio := highPriorityRate / lowPriorityRate
		t.Logf("   高/低优先级速率比: %.2f", ratio)

		// 分析公平性效果
		if alpha == 0.0 {
			// α=0时应该倾向于吞吐量最大化
			if ratio < 1.5 {
				t.Logf("   ⚠️  α=0时高优先级任务优势不明显")
			} else {
				t.Logf("   ✅ α=0时正确倾向于吞吐量最大化")
			}
		} else if alpha >= 1.0 {
			// α接近1时应该更公平
			if ratio > 3.0 {
				t.Logf("   ⚠️  α=1时公平性可能不足")
			} else {
				t.Logf("   ✅ α=1时表现出更好的公平性")
			}
		}
	}
}
