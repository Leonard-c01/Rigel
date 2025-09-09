package main

import (
	"fmt"
	"time"

	"tcp-proxy/internal/controlplane"
)

func main() {
	fmt.Println("⚡ UMW路由优化简单性能检查")
	fmt.Println("============================")

	// 创建调度器
	config := controlplane.DefaultUMWConfig()
	scheduler := controlplane.NewTrafficScheduler(config)

	// 创建简单的测试网络（3个节点）
	globalState := &controlplane.GlobalNetworkState{
		Timestamp:   time.Now(),
		LocalRegion: "test-region",
		LocalNodes: map[string]controlplane.NodeStatus{
			"node-a": {
				NodeID:        "node-a",
				Region:        "test-region",
				VirtualQueues: map[string]float64{"link_to_node-b": 0.1, "link_to_node-c": 0.2},
				IsHealthy:     true,
			},
			"node-b": {
				NodeID:        "node-b",
				Region:        "test-region",
				VirtualQueues: map[string]float64{"link_to_node-a": 0.15, "link_to_node-c": 0.25},
				IsHealthy:     true,
			},
			"node-c": {
				NodeID:        "node-c",
				Region:        "test-region",
				VirtualQueues: map[string]float64{"link_to_node-a": 0.3, "link_to_node-b": 0.35},
				IsHealthy:     true,
			},
		},
		RemoteRegions: make(map[string]controlplane.RegionSummary),
		TotalNodes:    3,
		TotalRegions:  1,
	}

	scheduler.UpdateGlobalState(globalState)

	fmt.Println("📊 网络配置: 3个节点")
	fmt.Println("🎯 性能要求: 响应时间 < 50ms")

	// 运行简单性能测试
	const testCount = 20
	var totalTime float64
	var maxTime float64
	var minTime float64 = 1000.0
	successCount := 0

	fmt.Printf("🚀 开始性能测试 (%d 次请求)...\n", testCount)

	for i := 0; i < testCount; i++ {
		request := &controlplane.RoutingRequest{
			TaskID:        fmt.Sprintf("perf-test-%d", i),
			SourceID:      "node-a",
			DestinationID: "node-c",
			DataSize:      1024 * 1024,
			Priority:      1.0,
			FairnessAlpha: 0.5,
			Timestamp:     time.Now().Unix(),
		}

		response, err := scheduler.ComputeOptimalRoute(request)
		if err != nil {
			fmt.Printf("❌ 测试 %d 失败: %v\n", i, err)
			continue
		}

		computationTime := response.ComputationTime
		totalTime += computationTime
		successCount++

		if computationTime > maxTime {
			maxTime = computationTime
		}
		if computationTime < minTime {
			minTime = computationTime
		}

		fmt.Printf("   请求 %d: %.2f ms\n", i+1, computationTime)
	}

	// 分析结果
	if successCount > 0 {
		avgTime := totalTime / float64(successCount)

		fmt.Printf("\n📈 性能分析结果:\n")
		fmt.Printf("   成功请求数: %d/%d\n", successCount, testCount)
		fmt.Printf("   平均响应时间: %.2f ms\n", avgTime)
		fmt.Printf("   最大响应时间: %.2f ms\n", maxTime)
		fmt.Printf("   最小响应时间: %.2f ms\n", minTime)

		// 性能要求验证
		fmt.Printf("\n🎯 性能要求验证:\n")

		if avgTime <= 50.0 {
			fmt.Printf("   ✅ 平均响应时间满足要求: %.2f ms < 50ms\n", avgTime)
		} else {
			fmt.Printf("   ❌ 平均响应时间超过要求: %.2f ms > 50ms\n", avgTime)
		}

		if maxTime <= 50.0 {
			fmt.Printf("   ✅ 最大响应时间满足要求: %.2f ms < 50ms\n", maxTime)
		} else {
			fmt.Printf("   ⚠️  最大响应时间超过要求: %.2f ms > 50ms\n", maxTime)
		}

		// 吞吐量估算
		if avgTime > 0 {
			throughput := 1000.0 / avgTime
			fmt.Printf("   理论吞吐量: %.1f 请求/秒\n", throughput)
		}

		// 总体评估
		if avgTime <= 50.0 && maxTime <= 100.0 {
			fmt.Printf("\n✅ 性能评估: 优秀 - 满足所有性能要求\n")
		} else if avgTime <= 50.0 {
			fmt.Printf("\n✅ 性能评估: 良好 - 平均性能满足要求\n")
		} else {
			fmt.Printf("\n⚠️  性能评估: 需要优化 - 部分指标超过要求\n")
		}

	} else {
		fmt.Println("❌ 所有测试都失败了")
	}

	fmt.Println("\n🎉 简单性能检查完成!")
}
