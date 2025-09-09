package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/rigel/internal/controlplane"
)

func main() {
	fmt.Println("=== Rigel 弹性伸缩演示 ===")
	
	// 1. 创建弹性控制器配置
	config := controlplane.DefaultElasticControllerConfig()
	
	// 添加模拟云服务商
	config.CloudProviders["mock"] = &controlplane.CloudProviderConfig{
		Provider: "mock",
		Region:   "test-region",
	}
	
	// 2. 创建弹性控制器
	controller := controlplane.NewElasticController(config)
	defer controller.Stop()
	
	// 3. 启动控制器
	if err := controller.Start(); err != nil {
		log.Fatalf("Failed to start elastic controller: %v", err)
	}
	
	fmt.Println("弹性控制器已启动")
	
	// 4. 演示基本功能
	demonstrateBasicScaling(controller)
	
	// 5. 演示波动性计算
	demonstrateVolatilityCalculation(controller)
	
	// 6. 演示状态机转换
	demonstrateStateMachine(controller)
	
	// 7. 演示云实例管理
	demonstrateCloudInstanceManagement(controller)
	
	// 8. 显示最终统计信息
	showFinalStats(controller)
	
	fmt.Println("演示完成")
}

// demonstrateBasicScaling 演示基本伸缩功能
func demonstrateBasicScaling(controller *controlplane.ElasticController) {
	fmt.Println("\n--- 基本伸缩功能演示 ---")
	
	nodeID := "demo-node-1"
	
	// 低虚拟队列值（不应触发伸缩）
	fmt.Printf("测试低虚拟队列值 (VQ=0.5)...\n")
	decision1, err := controller.ProcessScalingRequest(nodeID, 0.5)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}
	
	fmt.Printf("决策结果: ShouldScale=%t, CurrentState=%s, Reason=%s\n", 
		decision1.ShouldScale, decision1.CurrentState, decision1.Reason)
	
	// 连续高虚拟队列值（应触发伸缩）
	fmt.Printf("\n测试连续高虚拟队列值...\n")
	for i := 0; i < 5; i++ {
		vq := 3.0 + float64(i)*0.5
		decision, err := controller.ProcessScalingRequest(nodeID, vq)
		if err != nil {
			log.Printf("Error: %v", err)
			continue
		}
		
		fmt.Printf("第%d次: VQ=%.1f, ShouldScale=%t, State=%s->%s\n", 
			i+1, vq, decision.ShouldScale, decision.CurrentState, decision.TargetState)
		
		if decision.ShouldScale {
			fmt.Printf("  触发伸缩! 原因: %s\n", decision.Reason)
			break
		}
		
		time.Sleep(100 * time.Millisecond)
	}
}

// demonstrateVolatilityCalculation 演示波动性计算
func demonstrateVolatilityCalculation(controller *controlplane.ElasticController) {
	fmt.Println("\n--- 波动性计算演示 ---")
	
	nodeID := "demo-node-2"
	
	// 模拟虚拟队列的波动
	vqValues := []float64{0.5, 1.2, 2.8, 4.1, 3.5, 2.0, 1.5, 0.8}
	
	fmt.Println("虚拟队列波动序列:")
	for i, vq := range vqValues {
		decision, err := controller.SimulateDecision(nodeID, vq)
		if err != nil {
			log.Printf("Error: %v", err)
			continue
		}
		
		fmt.Printf("时隙%d: VQ=%.1f, Zi=%.3f, Pi=%.3f, DecisionVar=%.3f\n", 
			i+1, vq, decision.VolatilityQueue, decision.Perturbation, decision.DecisionVar)
		
		time.Sleep(50 * time.Millisecond)
	}
}

// demonstrateStateMachine 演示状态机转换
func demonstrateStateMachine(controller *controlplane.ElasticController) {
	fmt.Println("\n--- 状态机转换演示 ---")
	
	nodeID := "demo-node-3"
	
	// 演示完整的状态转换流程
	transitions := []struct {
		state  controlplane.ElasticNodeState
		reason string
	}{
		{controlplane.StateScalingUp, "开始创建云实例"},
		{controlplane.StateDormant, "实例创建完成"},
		{controlplane.StateTriggered, "检测到高负载"},
		{controlplane.StateDormant, "负载降低"},
		{controlplane.StateReleasing, "保留时间到期"},
		{controlplane.StateInactive, "实例已释放"},
	}
	
	for _, transition := range transitions {
		fmt.Printf("强制转换到状态: %s (%s)\n", transition.state, transition.reason)
		
		err := controller.ForceScaling(nodeID, transition.state, transition.reason)
		if err != nil {
			log.Printf("状态转换失败: %v", err)
			continue
		}
		
		// 获取当前状态
		nodeState, exists := controller.GetNodeState(nodeID)
		if exists {
			fmt.Printf("  当前状态: %s, 活动分数: %.2f\n", 
				nodeState.State, nodeState.ActivityScore)
		}
		
		time.Sleep(200 * time.Millisecond)
	}
}

// demonstrateCloudInstanceManagement 演示云实例管理
func demonstrateCloudInstanceManagement(controller *controlplane.ElasticController) {
	fmt.Println("\n--- 云实例管理演示 ---")
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	// 获取所有云实例
	instances, err := controller.GetCloudInstances(ctx)
	if err != nil {
		log.Printf("获取云实例失败: %v", err)
		return
	}
	
	fmt.Printf("当前云实例数量:\n")
	for provider, providerInstances := range instances {
		fmt.Printf("  %s: %d个实例\n", provider, len(providerInstances))
		for _, instance := range providerInstances {
			fmt.Printf("    - %s (%s) - NodeID: %s\n", 
				instance.InstanceID, instance.State, instance.NodeID)
		}
	}
}

// showFinalStats 显示最终统计信息
func showFinalStats(controller *controlplane.ElasticController) {
	fmt.Println("\n--- 最终统计信息 ---")
	
	// 控制器统计
	stats := controller.GetStats()
	fmt.Printf("弹性控制器统计:\n")
	fmt.Printf("  总决策次数: %d\n", stats.TotalScalingDecisions)
	fmt.Printf("  成功伸缩次数: %d\n", stats.SuccessfulScalings)
	fmt.Printf("  活跃节点数: %d\n", stats.ActiveNodes)
	fmt.Printf("  休眠节点数: %d\n", stats.DormantNodes)
	fmt.Printf("  平均波动性: %.3f\n", stats.AverageVolatility)
	fmt.Printf("  平均扰动: %.3f\n", stats.AveragePerturbation)
	
	// 状态统计
	stateStats := controller.GetStateStatistics()
	fmt.Printf("\n节点状态分布:\n")
	for state, count := range stateStats {
		if count > 0 {
			fmt.Printf("  %s: %d个节点\n", state, count)
		}
	}
	
	// 所有节点状态
	allNodes := controller.GetAllNodeStates()
	fmt.Printf("\n所有节点详情:\n")
	for nodeID, nodeInfo := range allNodes {
		fmt.Printf("  %s: %s (活动分数: %.2f, 保留时间: %.1fs)\n", 
			nodeID, nodeInfo.State, nodeInfo.ActivityScore, nodeInfo.RetainTime)
	}
}
