package main

import (
	"fmt"
	"time"

	"tcp-proxy/internal/controlplane"
)

func main() {
	fmt.Println("🔍 UMW调试测试")
	fmt.Println("===============")

	// 步骤1: 创建UMW配置
	fmt.Println("1. 创建UMW配置...")
	config := controlplane.DefaultUMWConfig()
	if config == nil {
		fmt.Println("❌ 配置创建失败")
		return
	}
	fmt.Printf("✅ 配置创建成功: WeightM=%.2f, PenaltyV=%.2f\n", config.WeightM, config.PenaltyV)

	// 步骤2: 创建TrafficScheduler
	fmt.Println("2. 创建TrafficScheduler...")
	scheduler := controlplane.NewTrafficScheduler(config)
	if scheduler == nil {
		fmt.Println("❌ TrafficScheduler创建失败")
		return
	}
	fmt.Println("✅ TrafficScheduler创建成功")

	// 步骤3: 创建简单的全局状态
	fmt.Println("3. 创建全局状态...")
	globalState := &controlplane.GlobalNetworkState{
		Timestamp:   time.Now(),
		LocalRegion: "test-region",
		LocalNodes: map[string]controlplane.NodeStatus{
			"node-a": {
				NodeID:            "node-a",
				Region:            "test-region",
				Address:           "127.0.0.1:9001",
				VirtualQueues:     map[string]float64{},
				IsHealthy:         true,
			},
			"node-b": {
				NodeID:            "node-b",
				Region:            "test-region",
				Address:           "127.0.0.1:9002",
				VirtualQueues:     map[string]float64{},
				IsHealthy:         true,
			},
		},
		RemoteRegions: make(map[string]controlplane.RegionSummary),
		TotalNodes:    2,
		TotalRegions:  1,
	}
	fmt.Println("✅ 全局状态创建成功")

	// 步骤4: 更新调度器状态
	fmt.Println("4. 更新调度器状态...")
	scheduler.UpdateGlobalState(globalState)
	fmt.Println("✅ 状态更新成功")

	// 步骤5: 创建简单的路由请求
	fmt.Println("5. 创建路由请求...")
	request := &controlplane.RoutingRequest{
		TaskID:        "debug-test-001",
		SourceID:      "node-a",
		DestinationID: "node-b",
		DataSize:      1024,
		Priority:      1.0,
		FairnessAlpha: 0.5,
		Timestamp:     time.Now().Unix(),
	}
	fmt.Printf("✅ 请求创建成功: %s -> %s\n", request.SourceID, request.DestinationID)

	// 步骤6: 尝试计算路由
	fmt.Println("6. 计算路由...")
	fmt.Println("   开始计算...")
	
	response, err := scheduler.ComputeOptimalRoute(request)
	if err != nil {
		fmt.Printf("❌ 路由计算失败: %v\n", err)
		return
	}

	fmt.Println("✅ 路由计算成功!")
	fmt.Printf("   路径: %v\n", response.OptimalPath)
	fmt.Printf("   速率: %.2f\n", response.RecommendedRate)
	fmt.Printf("   成本: %.4f\n", response.PathCost)
	fmt.Printf("   时间: %.2f ms\n", response.ComputationTime)

	fmt.Println("\n🎉 调试测试完成!")
}
