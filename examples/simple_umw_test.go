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

	fmt.Println("🚀 简单UMW路由测试")
	fmt.Println("==================")

	// 启动组长节点
	fmt.Println("启动组长节点...")
	leaderConfig := &controlplane.ControlPlaneConfig{
		NodeID:               "leader-test",
		Region:               "test-region",
		Address:              "127.0.0.1:9300",
		IsLeader:             true,
		StatusReportInterval: 5 * time.Second,
		GlobalSyncInterval:   2 * time.Second,
		LeaderSyncInterval:   2 * time.Second,
		HeartbeatTimeout:     15 * time.Second,
		RequestTimeout:       5 * time.Second,
		APIPort:              ":9300",
	}

	leaderManager := controlplane.NewControlPlaneManager(leaderConfig)
	if err := leaderManager.Start(); err != nil {
		fmt.Printf("Failed to start leader node: %v\n", err)
		return
	}
	defer leaderManager.Stop()

	fmt.Println("✅ 组长节点启动成功")

	// 启动几个普通节点
	nodeConfigs := []struct {
		id   string
		port string
	}{
		{"node-a", ":9301"},
		{"node-b", ":9302"},
		{"node-c", ":9303"},
	}

	var nodeManagers []*controlplane.ControlPlaneManager
	for _, config := range nodeConfigs {
		nodeConfig := &controlplane.ControlPlaneConfig{
			NodeID:               config.id,
			Region:               "test-region",
			Address:              "127.0.0.1" + config.port,
			IsLeader:             false,
			LeaderAddress:        "127.0.0.1:9300",
			StatusReportInterval: 5 * time.Second,
			GlobalSyncInterval:   2 * time.Second,
			HeartbeatTimeout:     15 * time.Second,
			RequestTimeout:       5 * time.Second,
		}

		manager := controlplane.NewControlPlaneManager(nodeConfig)
		if err := manager.Start(); err != nil {
			fmt.Printf("Failed to start node %s: %v\n", config.id, err)
			return
		}
		defer manager.Stop()
		nodeManagers = append(nodeManagers, manager)
		
		fmt.Printf("✅ 节点 %s 启动成功\n", config.id)
	}

	// 等待节点注册
	fmt.Println("等待节点注册和状态同步...")
	time.Sleep(8 * time.Second)

	// 测试路由API
	fmt.Println("\n🎯 测试UMW路由优化API")
	fmt.Println("========================")

	// 构造路由请求
	request := controlplane.RoutingRequest{
		TaskID:        "simple-test-001",
		SourceID:      "node-a",
		DestinationID: "node-c",
		DataSize:      1024 * 1024, // 1MB
		Priority:      1.0,
		FairnessAlpha: 0.5,
		Timestamp:     time.Now().Unix(),
	}

	fmt.Printf("发送路由请求: %s -> %s\n", request.SourceID, request.DestinationID)

	// 发送请求
	response, err := sendRoutingRequest(request)
	if err != nil {
		fmt.Printf("❌ 路由请求失败: %v\n", err)
		return
	}

	// 显示结果
	fmt.Println("\n✅ 路由计算成功!")
	fmt.Printf("📍 最优路径: %v\n", response.OptimalPath)
	fmt.Printf("🚀 推荐速率: %.2f KB/s\n", response.RecommendedRate/1024)
	fmt.Printf("💰 路径成本: %.4f\n", response.PathCost)
	fmt.Printf("⏱️  计算时间: %.2f ms\n", response.ComputationTime)
	fmt.Printf("🔧 算法版本: %s\n", response.AlgorithmVersion)

	// 验证性能要求
	if response.ComputationTime < 50.0 {
		fmt.Printf("✅ 满足性能要求 (< 50ms)\n")
	} else {
		fmt.Printf("⚠️  超过性能要求: %.2f ms > 50ms\n", response.ComputationTime)
	}

	// 测试多个请求
	fmt.Println("\n📊 测试连续请求")
	fmt.Println("================")

	for i := 1; i <= 5; i++ {
		request.TaskID = fmt.Sprintf("simple-test-%03d", i)
		request.Timestamp = time.Now().Unix()

		response, err := sendRoutingRequest(request)
		if err != nil {
			fmt.Printf("❌ 请求 %d 失败: %v\n", i, err)
			continue
		}

		fmt.Printf("请求 %d: 路径=%v, 成本=%.4f, 时间=%.2f ms\n",
			i, response.OptimalPath, response.PathCost, response.ComputationTime)

		time.Sleep(1 * time.Second)
	}

	// 测试不同优先级
	fmt.Println("\n⚖️  测试优先级效果")
	fmt.Println("==================")

	priorities := []float64{0.5, 1.0, 2.0}
	for _, priority := range priorities {
		request.TaskID = fmt.Sprintf("priority-test-%.1f", priority)
		request.Priority = priority
		request.Timestamp = time.Now().Unix()

		response, err := sendRoutingRequest(request)
		if err != nil {
			fmt.Printf("❌ 优先级 %.1f 测试失败: %v\n", priority, err)
			continue
		}

		fmt.Printf("优先级 %.1f: 推荐速率 %.2f KB/s\n",
			priority, response.RecommendedRate/1024)
	}

	fmt.Println("\n🎉 测试完成!")
}

// sendRoutingRequest 发送路由请求
func sendRoutingRequest(request controlplane.RoutingRequest) (*controlplane.RoutingResponse, error) {
	requestBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	resp, err := http.Post("http://127.0.0.1:9300/api/v1/routing/optimize",
		"application/json", bytes.NewBuffer(requestBody))
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
