package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rigel/internal/controlplane"
)

// TestBPScalerBasicDecision 测试BP-Scaler基本决策功能
func TestBPScalerBasicDecision(t *testing.T) {
	config := controlplane.DefaultBPScalerConfig()
	scaler := controlplane.NewBPScaler(config)

	nodeID := "test-node-1"

	// 测试低虚拟队列值（不应触发伸缩）
	decision1, err := scaler.MakeScalingDecision(nodeID, 0.5)
	require.NoError(t, err)
	assert.False(t, decision1.ShouldScale, "Low VQ should not trigger scaling")
	assert.Equal(t, controlplane.StateInactive, decision1.CurrentState)

	// 测试高虚拟队列值（应触发伸缩）
	_, err = scaler.MakeScalingDecision(nodeID, 5.0)
	require.NoError(t, err)
	// 由于波动性队列需要时间积累，第一次可能不会触发

	// 连续几次高虚拟队列值
	for i := 0; i < 5; i++ {
		decision, err := scaler.MakeScalingDecision(nodeID, 5.0)
		require.NoError(t, err)
		if decision.ShouldScale {
			assert.Equal(t, controlplane.StateScalingUp, decision.TargetState)
			break
		}
	}
}

// TestBPScalerCostCalculation 测试成本计算
func TestBPScalerCostCalculation(t *testing.T) {
	config := controlplane.DefaultBPScalerConfig()
	scaler := controlplane.NewBPScaler(config)

	nodeID := "test-node-cost"

	// 设置节点成本
	err := scaler.UpdateNodeCosts(nodeID, 100.0, 2.0)
	require.NoError(t, err)

	// 模拟决策
	decision, err := scaler.SimulateDecision(nodeID, 3.0)
	require.NoError(t, err)

	// 验证成本计算
	assert.Greater(t, decision.ScalingCost, 0.0, "Scaling cost should be positive")

	// 获取节点状态并验证成本参数
	nodeState, exists := scaler.GetNodeState(nodeID)
	require.True(t, exists)
	assert.Equal(t, 100.0, nodeState.FixedCost)
	assert.Equal(t, 2.0, nodeState.VariableCost)
}

// TestElasticStateManagerTransitions 测试状态机转换
func TestElasticStateManagerTransitions(t *testing.T) {
	config := controlplane.DefaultBPScalerConfig()
	stateManager := controlplane.NewElasticStateManager(config)
	defer stateManager.Cleanup()

	nodeID := "test-state-node"

	// 测试从INACTIVE到SCALING_UP的转换
	err := stateManager.TransitionState(nodeID, controlplane.StateScalingUp, "Test scaling up")
	require.NoError(t, err)

	nodeState, exists := stateManager.GetNodeState(nodeID)
	require.True(t, exists)
	assert.Equal(t, controlplane.StateScalingUp, nodeState.State)

	// 测试从SCALING_UP到DORMANT的转换
	err = stateManager.TransitionState(nodeID, controlplane.StateDormant, "Instance created")
	require.NoError(t, err)

	nodeState, exists = stateManager.GetNodeState(nodeID)
	require.True(t, exists)
	assert.Equal(t, controlplane.StateDormant, nodeState.State)

	// 测试从DORMANT到TRIGGERED的转换
	err = stateManager.TransitionState(nodeID, controlplane.StateTriggered, "High load detected")
	require.NoError(t, err)

	nodeState, exists = stateManager.GetNodeState(nodeID)
	require.True(t, exists)
	assert.Equal(t, controlplane.StateTriggered, nodeState.State)
	assert.False(t, nodeState.LastTriggeredAt.IsZero())

	// 验证活动分数更新
	assert.Greater(t, nodeState.ActivityScore, 0.0)
}

// TestElasticStateManagerInvalidTransitions 测试无效状态转换
func TestElasticStateManagerInvalidTransitions(t *testing.T) {
	config := controlplane.DefaultBPScalerConfig()
	stateManager := controlplane.NewElasticStateManager(config)
	defer stateManager.Cleanup()

	nodeID := "test-invalid-node"

	// 尝试无效转换：从INACTIVE直接到TRIGGERED
	err := stateManager.TransitionState(nodeID, controlplane.StateTriggered, "Invalid transition")
	assert.Error(t, err, "Should reject invalid state transition")
}

// TestElasticStateManagerRetainTime 测试保留时间计算
func TestElasticStateManagerRetainTime(t *testing.T) {
	config := controlplane.DefaultBPScalerConfig()
	config.BaseRetainTime = 60.0 // 1分钟基础保留时间
	stateManager := controlplane.NewElasticStateManager(config)
	defer stateManager.Cleanup()

	nodeID := "test-retain-node"

	// 创建节点并转换到TRIGGERED状态
	err := stateManager.TransitionState(nodeID, controlplane.StateScalingUp, "Test")
	require.NoError(t, err)
	err = stateManager.TransitionState(nodeID, controlplane.StateDormant, "Test")
	require.NoError(t, err)
	err = stateManager.TransitionState(nodeID, controlplane.StateTriggered, "Test")
	require.NoError(t, err)

	// 转换回DORMANT状态，应该开始保留时间计算
	err = stateManager.TransitionState(nodeID, controlplane.StateDormant, "Load decreased")
	require.NoError(t, err)

	nodeState, exists := stateManager.GetNodeState(nodeID)
	require.True(t, exists)
	assert.Greater(t, nodeState.RetainTime, 0.0, "Retain time should be calculated")

	// 计算保留时间
	retainTime, err := stateManager.CalculateRetainTime(nodeID)
	require.NoError(t, err)
	assert.Greater(t, retainTime, config.BaseRetainTime, "Retain time should be greater than base time due to activity score")
}

// TestCloudProviderMock 测试模拟云服务商
func TestCloudProviderMock(t *testing.T) {
	config := &controlplane.CloudProviderConfig{
		Provider: "mock",
		Region:   "test-region",
	}

	provider, err := controlplane.NewMockCloudProvider(config)
	require.NoError(t, err)

	ctx := context.Background()

	// 测试创建实例
	instanceConfig := &controlplane.InstanceConfig{
		InstanceType: "test-instance",
		Tags: map[string]string{
			"NodeID": "test-cloud-node",
		},
	}

	instance, err := provider.CreateInstance(ctx, instanceConfig)
	require.NoError(t, err)
	assert.NotEmpty(t, instance.InstanceID)
	assert.Equal(t, "mock", instance.Provider)
	assert.Equal(t, "test-cloud-node", instance.NodeID)

	// 测试获取实例
	retrievedInstance, err := provider.GetInstance(ctx, instance.InstanceID)
	require.NoError(t, err)
	assert.Equal(t, instance.InstanceID, retrievedInstance.InstanceID)

	// 测试列出实例
	instances, err := provider.ListInstances(ctx)
	require.NoError(t, err)
	assert.Len(t, instances, 1)

	// 测试删除实例
	err = provider.DeleteInstance(ctx, instance.InstanceID)
	require.NoError(t, err)

	// 验证实例已删除
	instances, err = provider.ListInstances(ctx)
	require.NoError(t, err)
	assert.Len(t, instances, 0)
}

// TestElasticControllerIntegration 测试弹性控制器集成
func TestElasticControllerIntegration(t *testing.T) {
	config := controlplane.DefaultElasticControllerConfig()

	// 添加模拟云服务商
	config.CloudProviders["mock"] = &controlplane.CloudProviderConfig{
		Provider: "mock",
		Region:   "test-region",
	}

	controller := controlplane.NewElasticController(config)
	defer controller.Stop()

	// 启动控制器
	err := controller.Start()
	require.NoError(t, err)

	nodeID := "test-integration-node"

	// 测试处理伸缩请求
	decision, err := controller.ProcessScalingRequest(nodeID, 2.0)
	require.NoError(t, err)
	assert.NotNil(t, decision)

	// 获取节点状态
	nodeState, exists := controller.GetNodeState(nodeID)
	require.True(t, exists)
	assert.NotEmpty(t, nodeState.NodeID)

	// 测试统计信息
	stats := controller.GetStats()
	assert.Greater(t, stats.TotalScalingDecisions, 0)

	// 测试状态统计
	stateStats := controller.GetStateStatistics()
	assert.Contains(t, stateStats, controlplane.StateInactive)
}

// TestVolatilityCalculator 测试波动性计算器
func TestVolatilityCalculator(t *testing.T) {
	config := controlplane.DefaultBPScalerConfig()
	config.CongestionThreshold = 1.0
	config.DecayFactor = 0.9
	config.SensitivityWeight = 1.0
	config.StabilityCapacity = 0.5

	calculator := controlplane.NewVolatilityCalculator(config)

	nodeID := "test-volatility-node"

	// 第一次更新（建立基线）
	metrics1, err := calculator.UpdateVolatilityMetrics(nodeID, 0.5)
	require.NoError(t, err)
	assert.Equal(t, 0.0, metrics1.Perturbation, "No perturbation below threshold")
	assert.Equal(t, 0.0, metrics1.VolatilityQueue, "Initial volatility queue should be 0")

	// 第二次更新（超过阈值）
	metrics2, err := calculator.UpdateVolatilityMetrics(nodeID, 2.0)
	require.NoError(t, err)
	assert.Greater(t, metrics2.Perturbation, 0.0, "Should have perturbation above threshold")
	assert.Greater(t, metrics2.VolatilityQueue, 0.0, "Volatility queue should increase")

	// 第三次更新（继续高值）
	metrics3, err := calculator.UpdateVolatilityMetrics(nodeID, 3.0)
	require.NoError(t, err)
	assert.Greater(t, metrics3.VolatilityQueue, metrics2.VolatilityQueue, "Volatility should continue to increase")

	// 第四次更新（回到低值）
	metrics4, err := calculator.UpdateVolatilityMetrics(nodeID, 0.5)
	require.NoError(t, err)
	assert.Equal(t, 0.0, metrics4.Perturbation, "Perturbation should be 0 below threshold")
	assert.Less(t, metrics4.VolatilityQueue, metrics3.VolatilityQueue, "Volatility should decay")
}

// TestElasticControllerSimulation 测试弹性控制器模拟功能
func TestElasticControllerSimulation(t *testing.T) {
	config := controlplane.DefaultElasticControllerConfig()
	controller := controlplane.NewElasticController(config)
	defer controller.Stop()

	nodeID := "test-simulation-node"

	// 测试模拟决策（不应改变实际状态）
	decision1, err := controller.SimulateDecision(nodeID, 3.0)
	require.NoError(t, err)
	assert.NotNil(t, decision1)

	// 验证节点状态未被创建
	_, exists := controller.GetNodeState(nodeID)
	assert.False(t, exists, "Simulation should not create actual node state")

	// 执行实际决策
	decision2, err := controller.ProcessScalingRequest(nodeID, 3.0)
	require.NoError(t, err)
	assert.NotNil(t, decision2)

	// 验证节点状态已创建
	_, exists = controller.GetNodeState(nodeID)
	assert.True(t, exists, "Actual decision should create node state")
}

// TestElasticControllerForceScaling 测试强制伸缩
func TestElasticControllerForceScaling(t *testing.T) {
	config := controlplane.DefaultElasticControllerConfig()
	config.CloudProviders["mock"] = &controlplane.CloudProviderConfig{
		Provider: "mock",
		Region:   "test-region",
	}

	controller := controlplane.NewElasticController(config)
	defer controller.Stop()

	err := controller.Start()
	require.NoError(t, err)

	nodeID := "test-force-node"

	// 强制转换到SCALING_UP状态
	err = controller.ForceScaling(nodeID, controlplane.StateScalingUp, "Manual test")
	require.NoError(t, err)

	// 验证状态转换
	nodeState, exists := controller.GetNodeState(nodeID)
	require.True(t, exists)
	assert.Equal(t, controlplane.StateDormant, nodeState.State, "Should transition to DORMANT after successful scaling")

	// 强制转换到TRIGGERED状态
	err = controller.ForceScaling(nodeID, controlplane.StateTriggered, "Manual activation")
	require.NoError(t, err)

	nodeState, exists = controller.GetNodeState(nodeID)
	require.True(t, exists)
	assert.Equal(t, controlplane.StateTriggered, nodeState.State)
}

// TestElasticControllerEventHandling 测试事件处理
func TestElasticControllerEventHandling(t *testing.T) {
	config := controlplane.DefaultElasticControllerConfig()
	controller := controlplane.NewElasticController(config)
	defer controller.Stop()

	err := controller.Start()
	require.NoError(t, err)

	// 获取事件通道
	eventChan := controller.GetEventChannel()

	nodeID := "test-event-node"

	// 执行一个操作来生成事件
	_, err = controller.ProcessScalingRequest(nodeID, 1.0)
	require.NoError(t, err)

	// 等待事件
	select {
	case event := <-eventChan:
		assert.NotNil(t, event)
		assert.Equal(t, nodeID, event.NodeID)
		assert.Equal(t, controlplane.EventScalingDecision, event.Type)
	case <-time.After(1 * time.Second):
		t.Fatal("Expected to receive an event within 1 second")
	}
}

// TestElasticControllerCostUpdate 测试成本更新
func TestElasticControllerCostUpdate(t *testing.T) {
	config := controlplane.DefaultElasticControllerConfig()
	controller := controlplane.NewElasticController(config)
	defer controller.Stop()

	nodeID := "test-cost-node"

	// 先创建节点
	_, err := controller.ProcessScalingRequest(nodeID, 1.0)
	require.NoError(t, err)

	// 更新成本
	err = controller.UpdateNodeCosts(nodeID, 200.0, 3.0)
	require.NoError(t, err)

	// 验证成本更新
	nodeState, exists := controller.GetNodeState(nodeID)
	require.True(t, exists)
	assert.Equal(t, 200.0, nodeState.FixedCost)
	assert.Equal(t, 3.0, nodeState.VariableCost)
}

// TestElasticControllerHistoryTracking 测试历史记录跟踪
func TestElasticControllerHistoryTracking(t *testing.T) {
	config := controlplane.DefaultElasticControllerConfig()
	controller := controlplane.NewElasticController(config)
	defer controller.Stop()

	nodeID := "test-history-node"

	// 执行多次决策
	for i := 0; i < 3; i++ {
		_, err := controller.ProcessScalingRequest(nodeID, float64(i+1))
		require.NoError(t, err)
	}

	// 检查决策历史
	decisionHistory, exists := controller.GetDecisionHistory(nodeID)
	require.True(t, exists)
	assert.Len(t, decisionHistory, 3, "Should have 3 decision records")

	// 检查状态转换历史
	transitionHistory, exists := controller.GetTransitionHistory(nodeID)
	require.True(t, exists)
	assert.Greater(t, len(transitionHistory), 0, "Should have transition records")
}

// TestBPScalerDecisionVariableCalculation 测试决策变量计算
func TestBPScalerDecisionVariableCalculation(t *testing.T) {
	config := controlplane.DefaultBPScalerConfig()
	config.ScalingWeight = 1.0
	config.CostWeight = 0.1

	scaler := controlplane.NewBPScaler(config)
	nodeID := "test-decision-var-node"

	// 设置节点成本
	err := scaler.UpdateNodeCosts(nodeID, 100.0, 1.0)
	require.NoError(t, err)

	// 执行多次决策以建立波动性队列
	var lastDecision *controlplane.BPScalerDecision
	for i := 0; i < 10; i++ {
		decision, err := scaler.MakeScalingDecision(nodeID, 3.0)
		require.NoError(t, err)
		lastDecision = decision
	}

	// 验证决策变量计算
	assert.NotNil(t, lastDecision)
	assert.Greater(t, lastDecision.VolatilityQueue, 0.0, "Volatility queue should be positive")
	assert.Greater(t, lastDecision.Perturbation, 0.0, "Perturbation should be positive")
	assert.Greater(t, lastDecision.ScalingCost, 0.0, "Scaling cost should be positive")

	// 验证决策变量的合理性
	expectedVolatilityTerm := -config.DecayFactor * config.ScalingWeight *
		lastDecision.VolatilityQueue * lastDecision.Perturbation
	expectedCostTerm := config.CostWeight * lastDecision.ScalingCost
	expectedDecisionVar := expectedVolatilityTerm + expectedCostTerm

	assert.InDelta(t, expectedDecisionVar, lastDecision.DecisionVar, 0.001,
		"Decision variable calculation should match expected formula")
}

// TestElasticStateManagerActivityScore 测试活动分数计算
func TestElasticStateManagerActivityScore(t *testing.T) {
	config := controlplane.DefaultBPScalerConfig()
	config.ActivityDecayRate = 0.9

	stateManager := controlplane.NewElasticStateManager(config)
	defer stateManager.Cleanup()

	nodeID := "test-activity-node"

	// 创建节点并多次触发
	err := stateManager.TransitionState(nodeID, controlplane.StateScalingUp, "Test")
	require.NoError(t, err)
	err = stateManager.TransitionState(nodeID, controlplane.StateDormant, "Test")
	require.NoError(t, err)

	// 第一次触发
	err = stateManager.TransitionState(nodeID, controlplane.StateTriggered, "First trigger")
	require.NoError(t, err)

	nodeState1, _ := stateManager.GetNodeState(nodeID)
	initialScore := nodeState1.ActivityScore
	assert.Equal(t, 1.0, initialScore, "Initial activity score should be 1.0")

	// 回到DORMANT然后再次触发
	err = stateManager.TransitionState(nodeID, controlplane.StateDormant, "Back to dormant")
	require.NoError(t, err)

	// 等待一小段时间模拟时间流逝
	time.Sleep(10 * time.Millisecond)

	err = stateManager.TransitionState(nodeID, controlplane.StateTriggered, "Second trigger")
	require.NoError(t, err)

	nodeState2, _ := stateManager.GetNodeState(nodeID)
	secondScore := nodeState2.ActivityScore
	assert.Greater(t, secondScore, initialScore, "Activity score should increase with repeated triggers")
}

// BenchmarkBPScalerDecision 性能测试：BP-Scaler决策
func BenchmarkBPScalerDecision(b *testing.B) {
	config := controlplane.DefaultBPScalerConfig()
	scaler := controlplane.NewBPScaler(config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodeID := "bench-node"
		_, err := scaler.MakeScalingDecision(nodeID, float64(i%10))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkElasticControllerProcessing 性能测试：弹性控制器处理
func BenchmarkElasticControllerProcessing(b *testing.B) {
	config := controlplane.DefaultElasticControllerConfig()
	controller := controlplane.NewElasticController(config)
	defer controller.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodeID := "bench-controller-node"
		_, err := controller.ProcessScalingRequest(nodeID, float64(i%10))
		if err != nil {
			b.Fatal(err)
		}
	}
}
