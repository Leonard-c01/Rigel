package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
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

	fmt.Println("🚀 Rigel UMW 路由优化服务演示")
	fmt.Println("===============================")

	// 启动控制平面环境
	fmt.Println("📡 启动控制平面环境...")

	// 启动组长节点
	go startLeaderNode()
	time.Sleep(2 * time.Second)

	// 启动多个普通节点
	nodeIDs := []string{"node-a", "node-b", "node-c", "node-d"}
	for i, nodeID := range nodeIDs {
		port := fmt.Sprintf(":920%d", i+1)
		go startRegularNode(nodeID, port)
		time.Sleep(500 * time.Millisecond)
	}

	// 等待节点注册和状态同步
	fmt.Println("⏳ 等待节点注册和状态同步...")
	time.Sleep(8 * time.Second)

	// 运行UMW路由优化演示
	fmt.Println("\n🎯 开始UMW路由优化演示")
	fmt.Println("========================")

	go runUMWRoutingDemo()
	go runVirtualQueueSimulation()
	go runFairnessDemo()

	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n👋 演示结束")
}

// startLeaderNode 启动组长节点
func startLeaderNode() {
	config := &controlplane.ControlPlaneConfig{
		NodeID:               "leader-demo",
		Region:               "demo-region",
		Address:              "127.0.0.1:9200",
		IsLeader:             true,
		StatusReportInterval: 3 * time.Second,
		GlobalSyncInterval:   1 * time.Second,
		LeaderSyncInterval:   2 * time.Second,
		HeartbeatTimeout:     15 * time.Second,
		RequestTimeout:       5 * time.Second,
		APIPort:              ":9200",
	}

	manager := controlplane.NewControlPlaneManager(config)
	if err := manager.Start(); err != nil {
		log.Errorf("Failed to start leader node: %v", err)
		return
	}

	fmt.Printf("👑 组长节点启动: %s\n", config.NodeID)

	// 保持运行
	select {}
}

// startRegularNode 启动普通节点
func startRegularNode(nodeID, apiPort string) {
	config := &controlplane.ControlPlaneConfig{
		NodeID:               nodeID,
		Region:               "demo-region",
		Address:              "127.0.0.1" + apiPort,
		IsLeader:             false,
		LeaderAddress:        "127.0.0.1:9200",
		StatusReportInterval: 3 * time.Second,
		GlobalSyncInterval:   1 * time.Second,
		HeartbeatTimeout:     15 * time.Second,
		RequestTimeout:       5 * time.Second,
	}

	manager := controlplane.NewControlPlaneManager(config)
	if err := manager.Start(); err != nil {
		log.Errorf("Failed to start regular node %s: %v", nodeID, err)
		return
	}

	fmt.Printf("🔗 普通节点启动: %s\n", nodeID)

	// 保持运行
	select {}
}

// runUMWRoutingDemo 运行UMW路由优化演示
func runUMWRoutingDemo() {
	time.Sleep(2 * time.Second)

	fmt.Println("\n📊 UMW路由优化基本功能演示")
	fmt.Println("============================")

	// 演示基本路由请求
	demoBasicRouting()

	time.Sleep(5 * time.Second)

	// 演示路径成本变化
	demoPathCostVariation()

	time.Sleep(5 * time.Second)

	// 演示算法性能
	demoAlgorithmPerformance()
}

// demoBasicRouting 演示基本路由功能
func demoBasicRouting() {
	fmt.Println("\n🎯 基本路由功能演示:")

	routes := []struct {
		source string
		dest   string
		desc   string
	}{
		{"node-a", "node-c", "短距离路由"},
		{"node-a", "node-d", "长距离路由"},
		{"node-b", "node-d", "跨节点路由"},
	}

	for i, route := range routes {
		fmt.Printf("\n--- 路由请求 %d: %s ---\n", i+1, route.desc)

		request := controlplane.RoutingRequest{
			TaskID:        fmt.Sprintf("demo-basic-%d", i+1),
			SourceID:      route.source,
			DestinationID: route.dest,
			DataSize:      1024 * 1024, // 1MB
			Priority:      1.0,
			FairnessAlpha: 0.5,
			Timestamp:     time.Now().Unix(),
		}

		response, err := sendRoutingRequest(request)
		if err != nil {
			fmt.Printf("❌ 路由请求失败: %v\n", err)
			continue
		}

		fmt.Printf("✅ 路由计算成功:\n")
		fmt.Printf("   📍 最优路径: %v\n", response.OptimalPath)
		fmt.Printf("   🚀 推荐速率: %.2f KB/s\n", response.RecommendedRate/1024)
		fmt.Printf("   💰 路径成本: %.4f\n", response.PathCost)
		fmt.Printf("   ⏱️  计算时间: %.2f ms\n", response.ComputationTime)
		fmt.Printf("   🔧 算法版本: %s\n", response.AlgorithmVersion)

		time.Sleep(2 * time.Second)
	}
}

// demoPathCostVariation 演示路径成本变化
func demoPathCostVariation() {
	fmt.Println("\n📈 路径成本变化演示:")
	fmt.Println("模拟网络拥塞对路由决策的影响...")

	baseRequest := controlplane.RoutingRequest{
		TaskID:        "demo-cost-variation",
		SourceID:      "node-a",
		DestinationID: "node-d",
		DataSize:      1024 * 1024,
		Priority:      1.0,
		FairnessAlpha: 0.5,
	}

	// 发送多个连续请求，观察成本变化
	fmt.Printf("\n发送连续路由请求，观察虚拟队列影响:\n")

	for i := 0; i < 5; i++ {
		baseRequest.TaskID = fmt.Sprintf("demo-cost-%d", i+1)
		baseRequest.Timestamp = time.Now().Unix()

		response, err := sendRoutingRequest(baseRequest)
		if err != nil {
			fmt.Printf("❌ 请求 %d 失败: %v\n", i+1, err)
			continue
		}

		fmt.Printf("📊 请求 %d: 路径=%v, 成本=%.4f, 速率=%.2f KB/s\n",
			i+1, response.OptimalPath, response.PathCost, response.RecommendedRate/1024)

		time.Sleep(1 * time.Second)
	}
}

// demoAlgorithmPerformance 演示算法性能
func demoAlgorithmPerformance() {
	fmt.Println("\n⚡ 算法性能演示:")
	fmt.Println("测试路由计算响应时间...")

	const numRequests = 10
	var totalTime float64
	var maxTime float64
	var minTime float64 = 1000.0

	for i := 0; i < numRequests; i++ {
		request := controlplane.RoutingRequest{
			TaskID:        fmt.Sprintf("perf-test-%d", i+1),
			SourceID:      "node-a",
			DestinationID: "node-c",
			DataSize:      1024 * 1024,
			Priority:      1.0,
			FairnessAlpha: 0.5,
			Timestamp:     time.Now().Unix(),
		}

		response, err := sendRoutingRequest(request)
		if err != nil {
			fmt.Printf("❌ 性能测试请求 %d 失败: %v\n", i+1, err)
			continue
		}

		computationTime := response.ComputationTime
		totalTime += computationTime

		if computationTime > maxTime {
			maxTime = computationTime
		}
		if computationTime < minTime {
			minTime = computationTime
		}

		fmt.Printf("⏱️  请求 %d: %.2f ms\n", i+1, computationTime)
	}

	avgTime := totalTime / numRequests
	fmt.Printf("\n📈 性能统计:\n")
	fmt.Printf("   平均响应时间: %.2f ms\n", avgTime)
	fmt.Printf("   最大响应时间: %.2f ms\n", maxTime)
	fmt.Printf("   最小响应时间: %.2f ms\n", minTime)

	// 验证是否满足性能要求（50ms）
	if avgTime < 50.0 {
		fmt.Printf("   ✅ 满足性能要求 (< 50ms)\n")
	} else {
		fmt.Printf("   ⚠️  超过性能要求 (50ms)\n")
	}
}

// sendRoutingRequest 发送路由请求
func sendRoutingRequest(request controlplane.RoutingRequest) (*controlplane.RoutingResponse, error) {
	requestBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	resp, err := http.Post("http://127.0.0.1:9200/api/v1/routing/optimize",
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

// runVirtualQueueSimulation 运行虚拟队列模拟
func runVirtualQueueSimulation() {
	time.Sleep(20 * time.Second)

	fmt.Println("\n🔄 虚拟队列影响演示")
	fmt.Println("====================")
	fmt.Println("模拟虚拟队列变化对路由决策的影响...")

	// 基础请求
	baseRequest := controlplane.RoutingRequest{
		TaskID:        "vq-demo-initial",
		SourceID:      "node-a",
		DestinationID: "node-d",
		DataSize:      1024 * 1024,
		Priority:      1.0,
		FairnessAlpha: 0.5,
	}

	// 获取初始路径
	fmt.Println("\n📍 获取初始路径:")
	initialResponse, err := sendRoutingRequest(baseRequest)
	if err != nil {
		fmt.Printf("❌ 初始请求失败: %v\n", err)
		return
	}

	fmt.Printf("   初始路径: %v\n", initialResponse.OptimalPath)
	fmt.Printf("   初始成本: %.4f\n", initialResponse.PathCost)

	// 模拟网络拥塞 - 连续发送请求增加虚拟队列
	fmt.Println("\n🚦 模拟网络拥塞 (连续发送请求):")
	for i := 0; i < 8; i++ {
		baseRequest.TaskID = fmt.Sprintf("vq-demo-load-%d", i+1)
		baseRequest.Timestamp = time.Now().Unix()

		response, err := sendRoutingRequest(baseRequest)
		if err != nil {
			fmt.Printf("❌ 拥塞模拟请求 %d 失败: %v\n", i+1, err)
			continue
		}

		fmt.Printf("   请求 %d: 路径=%v, 成本=%.4f\n",
			i+1, response.OptimalPath, response.PathCost)

		time.Sleep(500 * time.Millisecond)
	}

	// 等待虚拟队列稳定
	fmt.Println("\n⏳ 等待虚拟队列稳定...")
	time.Sleep(3 * time.Second)

	// 获取拥塞后的路径
	fmt.Println("\n📍 拥塞后路径:")
	baseRequest.TaskID = "vq-demo-after-congestion"
	baseRequest.Timestamp = time.Now().Unix()

	afterResponse, err := sendRoutingRequest(baseRequest)
	if err != nil {
		fmt.Printf("❌ 拥塞后请求失败: %v\n", err)
		return
	}

	fmt.Printf("   拥塞后路径: %v\n", afterResponse.OptimalPath)
	fmt.Printf("   拥塞后成本: %.4f\n", afterResponse.PathCost)

	// 分析变化
	costIncrease := afterResponse.PathCost - initialResponse.PathCost
	if costIncrease > 0 {
		fmt.Printf("   ✅ 虚拟队列影响验证: 成本增加 %.4f\n", costIncrease)
	} else {
		fmt.Printf("   ℹ️  成本变化: %.4f (可能由于网络拓扑或参数设置)\n", costIncrease)
	}
}

// runFairnessDemo 运行公平性演示
func runFairnessDemo() {
	time.Sleep(40 * time.Second)

	fmt.Println("\n⚖️  公平性参数演示")
	fmt.Println("==================")
	fmt.Println("测试不同α值对任务优先级的影响...")

	alphaValues := []float64{0.0, 0.5, 1.0}
	priorities := []float64{0.5, 1.0, 2.0}

	for _, alpha := range alphaValues {
		fmt.Printf("\n--- α = %.1f 的效果 ---\n", alpha)

		var results []struct {
			priority float64
			rate     float64
		}

		for _, priority := range priorities {
			request := controlplane.RoutingRequest{
				TaskID:        fmt.Sprintf("fairness-a%.1f-p%.1f", alpha, priority),
				SourceID:      "node-a",
				DestinationID: "node-c",
				DataSize:      1024 * 1024,
				Priority:      priority,
				FairnessAlpha: alpha,
				Timestamp:     time.Now().Unix(),
			}

			response, err := sendRoutingRequest(request)
			if err != nil {
				fmt.Printf("❌ 公平性测试失败 (α=%.1f, p=%.1f): %v\n", alpha, priority, err)
				continue
			}

			results = append(results, struct {
				priority float64
				rate     float64
			}{priority, response.RecommendedRate})

			fmt.Printf("   优先级 %.1f: 推荐速率 %.2f KB/s\n",
				priority, response.RecommendedRate/1024)

			time.Sleep(1 * time.Second)
		}

		// 分析公平性效果
		if len(results) >= 2 {
			lowRate := results[0].rate
			highRate := results[len(results)-1].rate

			if lowRate > 0 && highRate > 0 {
				ratio := highRate / lowRate
				fmt.Printf("   📊 高/低优先级速率比: %.2f\n", ratio)

				if alpha == 0.0 {
					fmt.Printf("   💡 α=0: 倾向于吞吐量最大化\n")
				} else if alpha >= 1.0 {
					fmt.Printf("   💡 α=1: 倾向于比例公平\n")
				} else {
					fmt.Printf("   💡 α=%.1f: 平衡吞吐量和公平性\n", alpha)
				}
			}
		}
	}

	fmt.Println("\n✅ 公平性参数演示完成")
}
