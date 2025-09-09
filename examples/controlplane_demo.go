package main

import (
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

	fmt.Println("🚀 Rigel 控制平面状态同步演示")
	fmt.Println("================================")

	// 启动组长节点
	go startLeaderNode()
	time.Sleep(2 * time.Second)

	// 启动普通节点
	go startRegularNode("node-1", ":9091")
	go startRegularNode("node-2", ":9092")
	go startRegularNode("node-3", ":9093")

	// 等待节点启动和注册
	time.Sleep(5 * time.Second)

	// 启动监控和演示
	go runMonitoring()
	go runDemo()

	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n👋 演示结束")
}

// startLeaderNode 启动组长节点
func startLeaderNode() {
	config := &controlplane.ControlPlaneConfig{
		NodeID:               "leader-us-east",
		Region:               "us-east",
		Address:              "127.0.0.1:9090",
		IsLeader:             true,
		StatusReportInterval: 3 * time.Second,
		GlobalSyncInterval:   1 * time.Second,
		LeaderSyncInterval:   2 * time.Second,
		HeartbeatTimeout:     15 * time.Second,
		RequestTimeout:       5 * time.Second,
		APIPort:              ":9090",
	}

	manager := controlplane.NewControlPlaneManager(config)
	if err := manager.Start(); err != nil {
		log.Errorf("Failed to start leader node: %v", err)
		return
	}

	fmt.Printf("👑 组长节点启动: %s (region: %s)\n", config.NodeID, config.Region)

	// 保持运行
	select {}
}

// startRegularNode 启动普通节点
func startRegularNode(nodeID, apiPort string) {
	config := &controlplane.ControlPlaneConfig{
		NodeID:               nodeID,
		Region:               "us-east",
		Address:              "127.0.0.1" + apiPort,
		IsLeader:             false,
		LeaderAddress:        "127.0.0.1:9090",
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

	// 模拟虚拟队列变化
	go simulateVirtualQueueChanges(manager, nodeID)

	// 保持运行
	select {}
}

// simulateVirtualQueueChanges 模拟虚拟队列变化
func simulateVirtualQueueChanges(manager *controlplane.ControlPlaneManager, nodeID string) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	linkID := fmt.Sprintf("link-%s", nodeID)
	value := 0.1

	for range ticker.C {
		value += 0.1
		if value > 1.0 {
			value = 0.1
		}
		
		manager.UpdateVirtualQueue(linkID, value)
		fmt.Printf("📊 节点 %s 更新虚拟队列 %s = %.2f\n", nodeID, linkID, value)
	}
}

// runMonitoring 运行监控
func runMonitoring() {
	ticker := time.NewTicker(8 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		fmt.Println("\n📈 当前系统状态:")
		fmt.Println("================")
		
		// 获取区域节点列表
		showRegionNodes()
		
		// 获取全局状态
		showGlobalState()
	}
}

// runDemo 运行演示场景
func runDemo() {
	time.Sleep(20 * time.Second)
	
	fmt.Println("\n🎭 演示场景: 节点故障模拟")
	fmt.Println("========================")
	
	// 这里可以添加更多演示场景
	// 比如模拟节点下线、网络分区等
}

// showRegionNodes 显示区域节点列表
func showRegionNodes() {
	resp, err := http.Get("http://127.0.0.1:9090/api/v1/region-nodes")
	if err != nil {
		fmt.Printf("❌ 获取区域节点失败: %v\n", err)
		return
	}
	defer resp.Body.Close()

	var response controlplane.RegionNodesResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		fmt.Printf("❌ 解析响应失败: %v\n", err)
		return
	}

	if !response.Success {
		fmt.Printf("❌ 请求失败: %s\n", response.Message)
		return
	}

	fmt.Printf("🏢 区域节点列表 (共 %d 个):\n", len(response.Nodes))
	for nodeID, node := range response.Nodes {
		status := "🟢"
		if !node.IsHealthy {
			status = "🔴"
		}
		
		lastSeen := time.Since(node.LastSeen).Truncate(time.Second)
		fmt.Printf("  %s %s - %s (最后心跳: %v前)\n", 
			status, nodeID, node.Address, lastSeen)
	}
}

// showGlobalState 显示全局状态
func showGlobalState() {
	resp, err := http.Get("http://127.0.0.1:9090/api/v1/global-status")
	if err != nil {
		fmt.Printf("❌ 获取全局状态失败: %v\n", err)
		return
	}
	defer resp.Body.Close()

	var response controlplane.GlobalStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		fmt.Printf("❌ 解析响应失败: %v\n", err)
		return
	}

	if !response.Success {
		fmt.Printf("❌ 请求失败: %s\n", response.Message)
		return
	}

	state := response.GlobalState
	fmt.Printf("🌐 全局网络状态:\n")
	fmt.Printf("  本地区域: %s\n", state.LocalRegion)
	fmt.Printf("  总节点数: %d\n", state.TotalNodes)
	fmt.Printf("  总区域数: %d\n", state.TotalRegions)

	if len(state.LocalNodes) > 0 {
		fmt.Printf("  本地节点详情:\n")
		for nodeID, status := range state.LocalNodes {
			fmt.Printf("    📍 %s: CPU=%.1f%%, 内存=%.1f%%, 虚拟队列=%d个\n",
				nodeID, status.CPUUsage*100, status.MemoryUsage*100, len(status.VirtualQueues))
			
			// 显示虚拟队列详情
			for linkID, queueValue := range status.VirtualQueues {
				fmt.Printf("      🔗 %s: %.3f\n", linkID, queueValue)
			}
		}
	}

	if len(state.RemoteRegions) > 0 {
		fmt.Printf("  远程区域:\n")
		for regionID, summary := range state.RemoteRegions {
			fmt.Printf("    🌍 %s: %d/%d 健康节点\n", 
				regionID, summary.HealthyNodes, summary.TotalNodes)
		}
	}
}
