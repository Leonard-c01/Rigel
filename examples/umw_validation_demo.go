package main

import (
	"fmt"
	"time"

	"tcp-proxy/internal/controlplane"
)

func main() {
	fmt.Println("🔍 UMW配置验证和错误处理演示")
	fmt.Println("================================")

	// 测试配置验证
	testConfigValidation()

	// 测试请求验证
	testRequestValidation()

	// 测试边界情况处理
	testEdgeCases()

	// 测试错误恢复
	testErrorRecovery()

	fmt.Println("\n🎉 验证演示完成!")
}

func testConfigValidation() {
	fmt.Println("\n1. 测试UMW配置验证")
	fmt.Println("===================")

	// 测试有效配置
	validConfig := &controlplane.UMWConfig{
		WeightM:       1.0,
		PenaltyV:      1.0,
		DefaultAlpha:  0.5,
		MaxIterations: 1000,
		Tolerance:     1e-6,
	}

	if err := controlplane.ValidateUMWConfig(validConfig); err != nil {
		fmt.Printf("❌ 有效配置验证失败: %v\n", err)
	} else {
		fmt.Println("✅ 有效配置验证通过")
	}

	// 测试无效配置
	invalidConfigs := []*controlplane.UMWConfig{
		nil, // nil配置
		{WeightM: -1.0, PenaltyV: 1.0, DefaultAlpha: 0.5, MaxIterations: 1000, Tolerance: 1e-6}, // 负权重
		{WeightM: 1.0, PenaltyV: 0.0, DefaultAlpha: 0.5, MaxIterations: 1000, Tolerance: 1e-6},  // 零惩罚
		{WeightM: 1.0, PenaltyV: 1.0, DefaultAlpha: 1.5, MaxIterations: 1000, Tolerance: 1e-6},  // 超范围alpha
		{WeightM: 1.0, PenaltyV: 1.0, DefaultAlpha: 0.5, MaxIterations: 0, Tolerance: 1e-6},     // 零迭代
		{WeightM: 1.0, PenaltyV: 1.0, DefaultAlpha: 0.5, MaxIterations: 1000, Tolerance: 0.0},   // 零容差
	}

	for i, config := range invalidConfigs {
		if err := controlplane.ValidateUMWConfig(config); err != nil {
			fmt.Printf("✅ 无效配置 %d 正确被拒绝: %v\n", i+1, err)
		} else {
			fmt.Printf("❌ 无效配置 %d 未被检测到\n", i+1)
		}
	}

	// 测试调度器对无效配置的处理
	fmt.Println("\n测试调度器对无效配置的处理:")
	invalidConfig := &controlplane.UMWConfig{WeightM: -1.0}
	scheduler := controlplane.NewTrafficScheduler(invalidConfig)
	if scheduler != nil {
		fmt.Println("✅ 调度器成功处理无效配置（使用默认配置）")
	}
}

func testRequestValidation() {
	fmt.Println("\n2. 测试路由请求验证")
	fmt.Println("===================")

	// 测试有效请求
	validRequest := &controlplane.RoutingRequest{
		TaskID:        "test-001",
		SourceID:      "node-a",
		DestinationID: "node-b",
		DataSize:      1024,
		Priority:      1.0,
		FairnessAlpha: 0.5,
		Timestamp:     time.Now().Unix(),
	}

	if err := controlplane.ValidateRoutingRequest(validRequest); err != nil {
		fmt.Printf("❌ 有效请求验证失败: %v\n", err)
	} else {
		fmt.Println("✅ 有效请求验证通过")
	}

	// 测试无效请求
	invalidRequests := []*controlplane.RoutingRequest{
		nil, // nil请求
		{TaskID: "test", SourceID: "", DestinationID: "node-b", DataSize: 1024, Priority: 1.0, FairnessAlpha: 0.5}, // 空源ID
		{TaskID: "test", SourceID: "node-a", DestinationID: "", DataSize: 1024, Priority: 1.0, FairnessAlpha: 0.5}, // 空目标ID
		{TaskID: "test", SourceID: "node-a", DestinationID: "node-a", DataSize: 1024, Priority: 1.0, FairnessAlpha: 0.5}, // 相同源目标
		{TaskID: "test", SourceID: "node-a", DestinationID: "node-b", DataSize: -1, Priority: 1.0, FairnessAlpha: 0.5}, // 负数据大小
		{TaskID: "test", SourceID: "node-a", DestinationID: "node-b", DataSize: 1024, Priority: 0.0, FairnessAlpha: 0.5}, // 零优先级
		{TaskID: "test", SourceID: "node-a", DestinationID: "node-b", DataSize: 1024, Priority: 1.0, FairnessAlpha: 1.5}, // 超范围alpha
	}

	for i, request := range invalidRequests {
		if err := controlplane.ValidateRoutingRequest(request); err != nil {
			fmt.Printf("✅ 无效请求 %d 正确被拒绝: %v\n", i+1, err)
		} else {
			fmt.Printf("❌ 无效请求 %d 未被检测到\n", i+1)
		}
	}
}

func testEdgeCases() {
	fmt.Println("\n3. 测试边界情况处理")
	fmt.Println("===================")

	// 创建调度器
	config := controlplane.DefaultUMWConfig()
	scheduler := controlplane.NewTrafficScheduler(config)

	// 测试空网络状态
	fmt.Println("测试空网络状态:")
	request := &controlplane.RoutingRequest{
		TaskID:        "edge-test-001",
		SourceID:      "node-a",
		DestinationID: "node-b",
		DataSize:      1024,
		Priority:      1.0,
		FairnessAlpha: 0.5,
		Timestamp:     time.Now().Unix(),
	}

	_, err := scheduler.ComputeOptimalRoute(request)
	if err != nil {
		fmt.Printf("✅ 空网络状态正确被处理: %v\n", err)
	} else {
		fmt.Println("❌ 空网络状态未被检测到")
	}

	// 测试不存在的节点
	fmt.Println("\n测试不存在的节点:")
	globalState := &controlplane.GlobalNetworkState{
		Timestamp:   time.Now(),
		LocalRegion: "test-region",
		LocalNodes: map[string]controlplane.NodeStatus{
			"node-x": {
				NodeID:        "node-x",
				Region:        "test-region",
				VirtualQueues: make(map[string]float64),
				IsHealthy:     true,
			},
		},
		RemoteRegions: make(map[string]controlplane.RegionSummary),
		TotalNodes:    1,
		TotalRegions:  1,
	}

	scheduler.UpdateGlobalState(globalState)
	_, err = scheduler.ComputeOptimalRoute(request)
	if err != nil {
		fmt.Printf("✅ 不存在的节点正确被处理: %v\n", err)
	} else {
		fmt.Println("❌ 不存在的节点未被检测到")
	}
}

func testErrorRecovery() {
	fmt.Println("\n4. 测试错误恢复能力")
	fmt.Println("===================")

	// 创建调度器
	config := controlplane.DefaultUMWConfig()
	scheduler := controlplane.NewTrafficScheduler(config)

	// 创建正常的网络状态
	globalState := &controlplane.GlobalNetworkState{
		Timestamp:   time.Now(),
		LocalRegion: "test-region",
		LocalNodes: map[string]controlplane.NodeStatus{
			"node-a": {
				NodeID:        "node-a",
				Region:        "test-region",
				VirtualQueues: make(map[string]float64),
				IsHealthy:     true,
			},
			"node-b": {
				NodeID:        "node-b",
				Region:        "test-region",
				VirtualQueues: make(map[string]float64),
				IsHealthy:     true,
			},
		},
		RemoteRegions: make(map[string]controlplane.RegionSummary),
		TotalNodes:    2,
		TotalRegions:  1,
	}

	scheduler.UpdateGlobalState(globalState)

	// 测试正常情况
	request := &controlplane.RoutingRequest{
		TaskID:        "recovery-test-001",
		SourceID:      "node-a",
		DestinationID: "node-b",
		DataSize:      1024,
		Priority:      1.0,
		FairnessAlpha: 0.5,
		Timestamp:     time.Now().Unix(),
	}

	response, err := scheduler.ComputeOptimalRoute(request)
	if err != nil {
		fmt.Printf("❌ 正常情况处理失败: %v\n", err)
	} else {
		fmt.Printf("✅ 正常情况处理成功: 路径=%v, 速率=%.2f\n", response.OptimalPath, response.RecommendedRate)
	}

	// 测试极端参数的处理
	fmt.Println("\n测试极端参数处理:")
	extremeRequest := &controlplane.RoutingRequest{
		TaskID:        "extreme-test-001",
		SourceID:      "node-a",
		DestinationID: "node-b",
		DataSize:      1024,
		Priority:      1e10, // 极大优先级
		FairnessAlpha: 0.0,  // 极端alpha值
		Timestamp:     time.Now().Unix(),
	}

	response, err = scheduler.ComputeOptimalRoute(extremeRequest)
	if err != nil {
		fmt.Printf("❌ 极端参数处理失败: %v\n", err)
	} else {
		fmt.Printf("✅ 极端参数处理成功: 路径=%v, 速率=%.2f\n", response.OptimalPath, response.RecommendedRate)
	}

	fmt.Println("\n✅ 错误恢复能力测试完成")
}
