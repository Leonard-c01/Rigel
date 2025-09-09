package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"tcp-proxy/internal/controlplane"
)

func main() {
	fmt.Println("⚡ UMW路由优化性能基准测试")
	fmt.Println("============================")

	// 运行不同规模的性能测试
	testScales := []struct {
		name      string
		nodeCount int
		testCount int
	}{
		{"小规模网络", 5, 100},
		{"中等规模网络", 10, 100},
		{"大规模网络", 20, 50},
	}

	for _, scale := range testScales {
		fmt.Printf("\n🔬 %s测试 (%d个节点, %d次请求)\n", scale.name, scale.nodeCount, scale.testCount)
		fmt.Println(strings.Repeat("=", 50))

		runPerformanceTest(scale.nodeCount, scale.testCount)
	}

	fmt.Println("\n🎯 性能要求验证")
	fmt.Println("================")
	fmt.Println("根据PRD要求，路由决策API响应时间应在50ms以内")

	// 运行专门的性能要求验证
	runPerformanceRequirementTest()
}

func runPerformanceTest(nodeCount, testCount int) {
	// 创建UMW配置
	config := controlplane.DefaultUMWConfig()
	scheduler := controlplane.NewTrafficScheduler(config)

	// 创建模拟网络状态
	globalState := createMockNetworkState(nodeCount)
	scheduler.UpdateGlobalState(globalState)

	fmt.Printf("📊 网络配置: %d个节点, %d条边\n", nodeCount, nodeCount*(nodeCount-1))

	// 收集性能数据
	var computationTimes []float64
	var pathLengths []int
	var pathCosts []float64

	fmt.Printf("🚀 开始性能测试...\n")
	startTime := time.Now()

	for i := 0; i < testCount; i++ {
		// 随机选择源和目标节点
		sourceID := fmt.Sprintf("node-%d", i%nodeCount)
		destID := fmt.Sprintf("node-%d", (i+nodeCount/2)%nodeCount)

		if sourceID == destID {
			destID = fmt.Sprintf("node-%d", (i+1)%nodeCount)
		}

		request := &controlplane.RoutingRequest{
			TaskID:        fmt.Sprintf("perf-test-%d", i),
			SourceID:      sourceID,
			DestinationID: destID,
			DataSize:      1024 * 1024,            // 1MB
			Priority:      1.0 + float64(i%3)*0.5, // 变化的优先级
			FairnessAlpha: 0.5,
			Timestamp:     time.Now().Unix(),
		}

		response, err := scheduler.ComputeOptimalRoute(request)
		if err != nil {
			fmt.Printf("❌ 测试 %d 失败: %v\n", i, err)
			continue
		}

		computationTimes = append(computationTimes, response.ComputationTime)
		pathLengths = append(pathLengths, len(response.OptimalPath))
		pathCosts = append(pathCosts, response.PathCost)

		// 每10次测试显示进度
		if (i+1)%10 == 0 {
			fmt.Printf("   进度: %d/%d (%.1f%%)\n", i+1, testCount, float64(i+1)/float64(testCount)*100)
		}
	}

	totalTime := time.Since(startTime)
	fmt.Printf("✅ 测试完成，总耗时: %v\n", totalTime)

	// 分析性能数据
	analyzePerformanceData(computationTimes, pathLengths, pathCosts)
}

func createMockNetworkState(nodeCount int) *controlplane.GlobalNetworkState {
	localNodes := make(map[string]controlplane.NodeStatus)

	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		virtualQueues := make(map[string]float64)

		// 为每个其他节点创建虚拟队列
		for j := 0; j < nodeCount; j++ {
			if i != j {
				linkID := fmt.Sprintf("link_to_node-%d", j)
				// 随机虚拟队列值，模拟不同的网络拥塞状况
				virtualQueues[linkID] = 0.1 + float64(i+j)*0.05
			}
		}

		localNodes[nodeID] = controlplane.NodeStatus{
			NodeID:            nodeID,
			Region:            "test-region",
			Address:           fmt.Sprintf("127.0.0.1:900%d", i),
			CPUUsage:          0.2 + float64(i)*0.05,
			MemoryUsage:       0.3 + float64(i)*0.03,
			InboundBandwidth:  1024 * 1024 * 10, // 10MB/s
			OutboundBandwidth: 1024 * 1024 * 10, // 10MB/s
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

func analyzePerformanceData(computationTimes []float64, pathLengths []int, pathCosts []float64) {
	if len(computationTimes) == 0 {
		fmt.Println("❌ 没有有效的性能数据")
		return
	}

	// 计算统计数据
	sort.Float64s(computationTimes)

	avgTime := calculateMean(computationTimes)
	medianTime := computationTimes[len(computationTimes)/2]
	p95Time := computationTimes[int(float64(len(computationTimes))*0.95)]
	p99Time := computationTimes[int(float64(len(computationTimes))*0.99)]
	maxTime := computationTimes[len(computationTimes)-1]
	minTime := computationTimes[0]

	avgPathLength := calculateMeanInt(pathLengths)
	avgPathCost := calculateMean(pathCosts)

	fmt.Printf("\n📈 性能分析结果:\n")
	fmt.Printf("   计算时间统计:\n")
	fmt.Printf("     平均时间: %.2f ms\n", avgTime)
	fmt.Printf("     中位数:   %.2f ms\n", medianTime)
	fmt.Printf("     P95:      %.2f ms\n", p95Time)
	fmt.Printf("     P99:      %.2f ms\n", p99Time)
	fmt.Printf("     最大时间: %.2f ms\n", maxTime)
	fmt.Printf("     最小时间: %.2f ms\n", minTime)

	fmt.Printf("   路径统计:\n")
	fmt.Printf("     平均路径长度: %.1f 跳\n", avgPathLength)
	fmt.Printf("     平均路径成本: %.4f\n", avgPathCost)

	// 性能要求验证
	fmt.Printf("\n🎯 性能要求验证:\n")

	passCount := 0
	for _, t := range computationTimes {
		if t <= 50.0 {
			passCount++
		}
	}

	passRate := float64(passCount) / float64(len(computationTimes)) * 100
	fmt.Printf("   50ms内完成率: %.1f%% (%d/%d)\n", passRate, passCount, len(computationTimes))

	if passRate >= 95.0 {
		fmt.Printf("   ✅ 优秀: 95%%以上请求在50ms内完成\n")
	} else if passRate >= 90.0 {
		fmt.Printf("   ✅ 良好: 90%%以上请求在50ms内完成\n")
	} else if passRate >= 80.0 {
		fmt.Printf("   ⚠️  一般: 80%%以上请求在50ms内完成\n")
	} else {
		fmt.Printf("   ❌ 需要优化: 仅%.1f%%请求在50ms内完成\n", passRate)
	}

	// 吞吐量计算
	totalRequests := float64(len(computationTimes))
	totalTime := avgTime * totalRequests / 1000.0 // 转换为秒
	throughput := totalRequests / totalTime
	fmt.Printf("   理论吞吐量: %.1f 请求/秒\n", throughput)
}

func runPerformanceRequirementTest() {
	fmt.Println("运行专门的50ms性能要求验证...")

	// 使用中等规模网络进行严格测试
	config := controlplane.DefaultUMWConfig()
	scheduler := controlplane.NewTrafficScheduler(config)
	globalState := createMockNetworkState(15) // 15个节点
	scheduler.UpdateGlobalState(globalState)

	const testCount = 200
	var results []float64

	for i := 0; i < testCount; i++ {
		request := &controlplane.RoutingRequest{
			TaskID:        fmt.Sprintf("req-test-%d", i),
			SourceID:      fmt.Sprintf("node-%d", i%15),
			DestinationID: fmt.Sprintf("node-%d", (i+7)%15),
			DataSize:      1024 * 1024,
			Priority:      1.0,
			FairnessAlpha: 0.5,
			Timestamp:     time.Now().Unix(),
		}

		response, err := scheduler.ComputeOptimalRoute(request)
		if err != nil {
			continue
		}

		results = append(results, response.ComputationTime)
	}

	// 分析结果
	sort.Float64s(results)
	avgTime := calculateMean(results)
	maxTime := results[len(results)-1]

	passCount := 0
	for _, t := range results {
		if t <= 50.0 {
			passCount++
		}
	}

	passRate := float64(passCount) / float64(len(results)) * 100

	fmt.Printf("\n📊 性能要求验证结果:\n")
	fmt.Printf("   测试请求数: %d\n", len(results))
	fmt.Printf("   平均响应时间: %.2f ms\n", avgTime)
	fmt.Printf("   最大响应时间: %.2f ms\n", maxTime)
	fmt.Printf("   50ms达标率: %.1f%%\n", passRate)

	if avgTime <= 50.0 && passRate >= 95.0 {
		fmt.Printf("   ✅ 完全满足性能要求\n")
	} else if avgTime <= 50.0 {
		fmt.Printf("   ✅ 基本满足性能要求\n")
	} else {
		fmt.Printf("   ❌ 未满足性能要求，需要优化\n")
	}
}

// 辅助函数
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

func calculateMeanInt(values []int) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0
	for _, v := range values {
		sum += v
	}
	return float64(sum) / float64(len(values))
}
