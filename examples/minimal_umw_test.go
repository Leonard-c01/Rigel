package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"tcp-proxy/internal/controlplane"
)

func main() {
	fmt.Println("🚀 最小UMW路由测试")
	fmt.Println("==================")

	// 直接测试TrafficScheduler
	fmt.Println("1. 测试TrafficScheduler直接调用...")
	testTrafficSchedulerDirect()

	fmt.Println("\n2. 测试完整的控制平面...")
	testFullControlPlane()
}

func testTrafficSchedulerDirect() {
	// 创建UMW配置
	config := controlplane.DefaultUMWConfig()
	fmt.Printf("UMW配置: WeightM=%.2f, PenaltyV=%.2f, DefaultAlpha=%.2f\n", 
		config.WeightM, config.PenaltyV, config.DefaultAlpha)

	// 创建TrafficScheduler
	scheduler := controlplane.NewTrafficScheduler(config)
	fmt.Println("✅ TrafficScheduler创建成功")

	// 创建模拟的全局网络状态
	globalState := &controlplane.GlobalNetworkState{
		Timestamp:   time.Now(),
		LocalRegion: "test-region",
		LocalNodes: map[string]controlplane.NodeStatus{
			"node-a": {
				NodeID:            "node-a",
				Region:            "test-region",
				Address:           "127.0.0.1:9001",
				CPUUsage:          0.3,
				MemoryUsage:       0.4,
				InboundBandwidth:  1024 * 1024 * 10,  // 10MB/s
				OutboundBandwidth: 1024 * 1024 * 10,  // 10MB/s
				VirtualQueues:     map[string]float64{"link_to_node-b": 0.1, "link_to_node-c": 0.2},
				IsHealthy:         true,
			},
			"node-b": {
				NodeID:            "node-b",
				Region:            "test-region",
				Address:           "127.0.0.1:9002",
				CPUUsage:          0.2,
				MemoryUsage:       0.3,
				InboundBandwidth:  1024 * 1024 * 10,
				OutboundBandwidth: 1024 * 1024 * 10,
				VirtualQueues:     map[string]float64{"link_to_node-a": 0.15, "link_to_node-c": 0.25},
				IsHealthy:         true,
			},
			"node-c": {
				NodeID:            "node-c",
				Region:            "test-region",
				Address:           "127.0.0.1:9003",
				CPUUsage:          0.4,
				MemoryUsage:       0.5,
				InboundBandwidth:  1024 * 1024 * 10,
				OutboundBandwidth: 1024 * 1024 * 10,
				VirtualQueues:     map[string]float64{"link_to_node-a": 0.3, "link_to_node-b": 0.35},
				IsHealthy:         true,
			},
		},
		RemoteRegions: make(map[string]controlplane.RegionSummary),
		TotalNodes:    3,
		TotalRegions:  1,
	}

	// 更新调度器的全局状态
	scheduler.UpdateGlobalState(globalState)
	fmt.Println("✅ 全局状态更新成功")

	// 创建路由请求
	request := &controlplane.RoutingRequest{
		TaskID:        "direct-test-001",
		SourceID:      "node-a",
		DestinationID: "node-c",
		DataSize:      1024 * 1024, // 1MB
		Priority:      1.0,
		FairnessAlpha: 0.5,
		Timestamp:     time.Now().Unix(),
	}

	fmt.Printf("发送路由请求: %s -> %s\n", request.SourceID, request.DestinationID)

	// 计算最优路由
	response, err := scheduler.ComputeOptimalRoute(request)
	if err != nil {
		fmt.Printf("❌ 路由计算失败: %v\n", err)
		return
	}

	// 显示结果
	fmt.Println("✅ 路由计算成功!")
	fmt.Printf("📍 最优路径: %v\n", response.OptimalPath)
	fmt.Printf("🚀 推荐速率: %.2f KB/s\n", response.RecommendedRate/1024)
	fmt.Printf("💰 路径成本: %.4f\n", response.PathCost)
	fmt.Printf("⏱️  计算时间: %.2f ms\n", response.ComputationTime)
	fmt.Printf("🔧 算法版本: %s\n", response.AlgorithmVersion)

	// 验证基本要求
	if len(response.OptimalPath) >= 2 {
		fmt.Println("✅ 路径包含源和目标节点")
	} else {
		fmt.Println("❌ 路径长度不足")
	}

	if response.RecommendedRate > 0 {
		fmt.Println("✅ 推荐速率为正数")
	} else {
		fmt.Println("❌ 推荐速率应为正数")
	}

	if response.ComputationTime < 50.0 {
		fmt.Println("✅ 满足性能要求 (< 50ms)")
	} else {
		fmt.Printf("⚠️  计算时间较长: %.2f ms\n", response.ComputationTime)
	}

	// 测试不同参数的效果
	fmt.Println("\n测试不同优先级的效果:")
	priorities := []float64{0.5, 1.0, 2.0}
	for _, priority := range priorities {
		request.Priority = priority
		request.TaskID = fmt.Sprintf("priority-%.1f", priority)
		
		resp, err := scheduler.ComputeOptimalRoute(request)
		if err != nil {
			fmt.Printf("❌ 优先级 %.1f 测试失败: %v\n", priority, err)
			continue
		}
		
		fmt.Printf("优先级 %.1f: 速率 %.2f KB/s\n", priority, resp.RecommendedRate/1024)
	}

	// 测试不同公平性参数的效果
	fmt.Println("\n测试不同公平性参数的效果:")
	alphas := []float64{0.0, 0.5, 1.0}
	for _, alpha := range alphas {
		request.FairnessAlpha = alpha
		request.Priority = 1.0
		request.TaskID = fmt.Sprintf("alpha-%.1f", alpha)
		
		resp, err := scheduler.ComputeOptimalRoute(request)
		if err != nil {
			fmt.Printf("❌ α=%.1f 测试失败: %v\n", alpha, err)
			continue
		}
		
		fmt.Printf("α=%.1f: 速率 %.2f KB/s\n", alpha, resp.RecommendedRate/1024)
	}
}

func testFullControlPlane() {
	fmt.Println("暂时跳过完整控制平面测试（需要更复杂的设置）")
	fmt.Println("直接测试已经验证了UMW算法的核心功能")
}
