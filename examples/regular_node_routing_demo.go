package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"tcp-proxy/internal/controlplane"
	"tcp-proxy/pkg/log"
)

func main() {
	// 初始化日志
	if err := log.Init("info", ""); err != nil {
		fmt.Printf("Failed to initialize logger: %v\n", err)
		return
	}

	fmt.Println("🔗 普通节点路由优化服务演示")
	fmt.Println("============================")

	// 启动组长节点
	fmt.Println("1. 启动组长节点...")
	leaderConfig := &controlplane.ControlPlaneConfig{
		NodeID:               "leader-demo",
		Region:               "demo-region",
		Address:              "127.0.0.1:9400",
		IsLeader:             true,
		StatusReportInterval: 5 * time.Second,
		GlobalSyncInterval:   2 * time.Second,
		LeaderSyncInterval:   2 * time.Second,
		HeartbeatTimeout:     15 * time.Second,
		RequestTimeout:       5 * time.Second,
		APIPort:              ":9400",
	}

	leaderManager := controlplane.NewControlPlaneManager(leaderConfig)
	if err := leaderManager.Start(); err != nil {
		fmt.Printf("Failed to start leader node: %v\n", err)
		return
	}
	defer leaderManager.Stop()
	fmt.Println("✅ 组长节点启动成功")

	// 启动普通节点（带API服务）
	fmt.Println("2. 启动普通节点...")
	nodeConfigs := []struct {
		id      string
		apiPort string
	}{
		{"node-a", ":9401"},
		{"node-b", ":9402"},
		{"node-c", ":9403"},
	}

	var nodeManagers []*controlplane.ControlPlaneManager
	for _, config := range nodeConfigs {
		nodeConfig := &controlplane.ControlPlaneConfig{
			NodeID:               config.id,
			Region:               "demo-region",
			Address:              "127.0.0.1" + config.apiPort,
			IsLeader:             false,
			LeaderAddress:        "127.0.0.1:9400",
			StatusReportInterval: 5 * time.Second,
			GlobalSyncInterval:   2 * time.Second,
			HeartbeatTimeout:     15 * time.Second,
			RequestTimeout:       5 * time.Second,
			APIPort:              config.apiPort, // 为普通节点配置API端口
		}

		manager := controlplane.NewControlPlaneManager(nodeConfig)
		if err := manager.Start(); err != nil {
			fmt.Printf("Failed to start node %s: %v\n", config.id, err)
			return
		}
		defer manager.Stop()
		nodeManagers = append(nodeManagers, manager)
		
		fmt.Printf("✅ 普通节点 %s 启动成功 (API: %s)\n", config.id, config.apiPort)
	}

	// 等待节点注册和状态同步
	fmt.Println("3. 等待节点注册和状态同步...")
	time.Sleep(8 * time.Second)

	// 测试组长节点的路由服务
	fmt.Println("\n🎯 测试组长节点路由服务")
	fmt.Println("========================")
	testRoutingService("组长节点", "http://127.0.0.1:9400/api/v1/routing/optimize")

	// 测试普通节点的路由服务
	fmt.Println("\n🎯 测试普通节点路由服务")
	fmt.Println("========================")
	
	for i, config := range nodeConfigs {
		fmt.Printf("\n--- 测试节点 %s ---\n", config.id)
		url := fmt.Sprintf("http://127.0.0.1%s/api/v1/routing/optimize", config.apiPort)
		testRoutingService(config.id, url)
		
		// 测试节点状态查询
		statusURL := fmt.Sprintf("http://127.0.0.1%s/api/v1/node/status", config.apiPort)
		testNodeStatus(config.id, statusURL)
		
		if i < len(nodeConfigs)-1 {
			time.Sleep(2 * time.Second)
		}
	}

	// 性能对比测试
	fmt.Println("\n⚡ 性能对比演示")
	fmt.Println("================")
	comparePerformance()

	fmt.Println("\n🎉 演示完成!")
	fmt.Println("✅ 普通节点现在也可以提供路由优化服务")
	fmt.Println("✅ 这提高了系统的可用性和负载分散能力")
}

func testRoutingService(nodeName, url string) {
	request := controlplane.RoutingRequest{
		TaskID:        fmt.Sprintf("demo-%s", nodeName),
		SourceID:      "node-a",
		DestinationID: "node-c",
		DataSize:      1024 * 1024, // 1MB
		Priority:      1.0,
		FairnessAlpha: 0.5,
		Timestamp:     time.Now().Unix(),
	}

	response, err := sendRoutingRequest(url, request)
	if err != nil {
		fmt.Printf("❌ %s 路由请求失败: %v\n", nodeName, err)
		return
	}

	fmt.Printf("✅ %s 路由计算成功:\n", nodeName)
	fmt.Printf("   📍 最优路径: %v\n", response.OptimalPath)
	fmt.Printf("   🚀 推荐速率: %.2f KB/s\n", response.RecommendedRate/1024)
	fmt.Printf("   💰 路径成本: %.4f\n", response.PathCost)
	fmt.Printf("   ⏱️  计算时间: %.2f ms\n", response.ComputationTime)
}

func testNodeStatus(nodeName, url string) {
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("❌ %s 状态查询失败: %v\n", nodeName, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("❌ %s 状态查询失败，状态码: %d\n", nodeName, resp.StatusCode)
		return
	}

	var status map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		fmt.Printf("❌ %s 状态解析失败: %v\n", nodeName, err)
		return
	}

	fmt.Printf("📊 %s 状态信息:\n", nodeName)
	if schedulerStats, ok := status["scheduler_stats"].(map[string]interface{}); ok {
		if totalRequests, ok := schedulerStats["total_requests"].(float64); ok {
			fmt.Printf("   总请求数: %.0f\n", totalRequests)
		}
		if avgLatency, ok := schedulerStats["average_latency"].(float64); ok {
			fmt.Printf("   平均延迟: %.2f ms\n", avgLatency)
		}
	}
}

func comparePerformance() {
	urls := []struct {
		name string
		url  string
	}{
		{"组长节点", "http://127.0.0.1:9400/api/v1/routing/optimize"},
		{"普通节点A", "http://127.0.0.1:9401/api/v1/routing/optimize"},
		{"普通节点B", "http://127.0.0.1:9402/api/v1/routing/optimize"},
	}

	const testCount = 10

	for _, target := range urls {
		fmt.Printf("\n测试 %s 性能:\n", target.name)
		
		var totalTime float64
		successCount := 0

		for i := 0; i < testCount; i++ {
			request := controlplane.RoutingRequest{
				TaskID:        fmt.Sprintf("perf-%s-%d", target.name, i),
				SourceID:      "node-a",
				DestinationID: "node-c",
				DataSize:      1024 * 1024,
				Priority:      1.0,
				FairnessAlpha: 0.5,
				Timestamp:     time.Now().Unix(),
			}

			response, err := sendRoutingRequest(target.url, request)
			if err != nil {
				continue
			}

			totalTime += response.ComputationTime
			successCount++
		}

		if successCount > 0 {
			avgTime := totalTime / float64(successCount)
			fmt.Printf("   平均响应时间: %.2f ms (%d/%d 成功)\n", avgTime, successCount, testCount)
		} else {
			fmt.Printf("   ❌ 所有请求都失败了\n")
		}
	}
}

func sendRoutingRequest(url string, request controlplane.RoutingRequest) (*controlplane.RoutingResponse, error) {
	requestBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request failed with status: %d", resp.StatusCode)
	}

	var response controlplane.RoutingResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}

	return &response, nil
}
