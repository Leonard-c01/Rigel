package main

import (
	"fmt"
	"sort"
	"time"

	"tcp-proxy/internal/controlplane"
)

func main() {
	fmt.Println("⚡ UMW路由优化快速性能测试")
	fmt.Println("============================")

	// 创建调度器
	config := controlplane.DefaultUMWConfig()
	scheduler := controlplane.NewTrafficScheduler(config)

	// 创建测试网络（10个节点）
	globalState := createTestNetwork(10)
	scheduler.UpdateGlobalState(globalState)

	fmt.Println("📊 网络配置: 10个节点")
	fmt.Println("🎯 性能要求: 响应时间 < 50ms")

	// 运行性能测试
	const testCount = 50
	var computationTimes []float64

	fmt.Printf("🚀 开始性能测试 (%d 次请求)...\n", testCount)

	for i := 0; i < testCount; i++ {
		sourceID := fmt.Sprintf("node-%d", i%10)
		destID := fmt.Sprintf("node-%d", (i+5)%10)

		request := &controlplane.RoutingRequest{
			TaskID:        fmt.Sprintf("perf-test-%d", i),
			SourceID:      sourceID,
			DestinationID: destID,
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

		computationTimes = append(computationTimes, response.ComputationTime)

		if (i+1)%10 == 0 {
			fmt.Printf("   进度: %d/%d\n", i+1, testCount)
		}
	}

	// 分析结果
	analyzeResults(computationTimes)

	fmt.Println("\n🎉 快速性能测试完成!")
}

func createTestNetwork(nodeCount int) *controlplane.GlobalNetworkState {
	localNodes := make(map[string]controlplane.NodeStatus)

	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		virtualQueues := make(map[string]float64)

		// 为每个其他节点创建虚拟队列
		for j := 0; j < nodeCount; j++ {
			if i != j {
				linkID := fmt.Sprintf("link_to_node-%d", j)
				virtualQueues[linkID] = 0.1 + float64(i+j)*0.02
			}
		}

		localNodes[nodeID] = controlplane.NodeStatus{
			NodeID:            nodeID,
			Region:            "test-region",
			Address:           fmt.Sprintf("127.0.0.1:900%d", i),
			CPUUsage:          0.2 + float64(i)*0.03,
			MemoryUsage:       0.3 + float64(i)*0.02,
			InboundBandwidth:  1024 * 1024 * 10,
			OutboundBandwidth: 1024 * 1024 * 10,
			VirtualQueues:     virtualQueues,
			IsHealthy:         true,
		}
	}

	return &controlplane.GlobalNetworkState{
		Timestamp:     time.Now(),
		LocalRegion:   "test-region",
		LocalNodes:    localNodes,
		RemoteRegions: make(map[string]controlplane.RegionSummary),
		TotalNodes:    nodeCount,
		TotalRegions:  1,
	}
}

func analyzeResults(computationTimes []float64) {
	if len(computationTimes) == 0 {
		fmt.Println("❌ 没有有效的测试数据")
		return
	}

	sort.Float64s(computationTimes)

	avgTime := calculateMean(computationTimes)
	medianTime := computationTimes[len(computationTimes)/2]
	maxTime := computationTimes[len(computationTimes)-1]
	minTime := computationTimes[0]

	fmt.Printf("\n📈 性能分析结果:\n")
	fmt.Printf("   测试请求数: %d\n", len(computationTimes))
	fmt.Printf("   平均响应时间: %.2f ms\n", avgTime)
	fmt.Printf("   中位数响应时间: %.2f ms\n", medianTime)
	fmt.Printf("   最大响应时间: %.2f ms\n", maxTime)
	fmt.Printf("   最小响应时间: %.2f ms\n", minTime)

	// 性能要求验证
	fmt.Printf("\n🎯 性能要求验证:\n")

	passCount := 0
	for _, t := range computationTimes {
		if t <= 50.0 {
			passCount++
		}
	}

	passRate := float64(passCount) / float64(len(computationTimes)) * 100
	fmt.Printf("   50ms达标率: %.1f%% (%d/%d)\n", passRate, passCount, len(computationTimes))

	if avgTime <= 50.0 && passRate >= 95.0 {
		fmt.Printf("   ✅ 优秀: 平均时间%.2fms，95%%以上请求达标\n", avgTime)
	} else if avgTime <= 50.0 {
		fmt.Printf("   ✅ 良好: 平均时间%.2fms达标\n", avgTime)
	} else {
		fmt.Printf("   ❌ 需要优化: 平均时间%.2fms超过要求\n", avgTime)
	}

	// 吞吐量估算
	if avgTime > 0 {
		throughput := 1000.0 / avgTime // 每秒请求数
		fmt.Printf("   理论吞吐量: %.1f 请求/秒\n", throughput)
	}

	// 延迟分布
	p95 := computationTimes[int(float64(len(computationTimes))*0.95)]
	p99 := computationTimes[int(float64(len(computationTimes))*0.99)]
	fmt.Printf("   P95延迟: %.2f ms\n", p95)
	fmt.Printf("   P99延迟: %.2f ms\n", p99)

	if p95 <= 50.0 {
		fmt.Printf("   ✅ P95延迟满足要求\n")
	} else {
		fmt.Printf("   ⚠️  P95延迟超过要求\n")
	}
}

func calculateMean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}
