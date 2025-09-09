package controlplane

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ElasticController 弹性伸缩主控制器
// 整合BP-Scaler、状态机管理和云服务商接口，提供完整的弹性伸缩功能
type ElasticController struct {
	config *ElasticControllerConfig

	// 核心组件
	bpScaler     *BPScaler
	stateManager *ElasticStateManager
	cloudManager *CloudManager

	// 工作协程管理
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 统计信息
	stats *ElasticControllerStats
	mu    sync.RWMutex

	// 事件通道
	eventChan chan *ElasticEvent
}

// ElasticControllerConfig 弹性控制器配置
type ElasticControllerConfig struct {
	// BP-Scaler配置
	BPScalerConfig *BPScalerConfig `json:"bp_scaler_config"`

	// 决策和监控周期
	DecisionInterval   time.Duration `json:"decision_interval"`   // 决策周期
	MonitoringInterval time.Duration `json:"monitoring_interval"` // 监控周期

	// 云服务商配置
	CloudProviders map[string]*CloudProviderConfig `json:"cloud_providers"`

	// 默认实例配置
	DefaultInstanceConfig *InstanceConfig `json:"default_instance_config"`

	// 事件缓冲区大小
	EventBufferSize int `json:"event_buffer_size"`
}

// ElasticEvent 弹性伸缩事件
type ElasticEvent struct {
	Type      ElasticEventType `json:"type"`
	NodeID    string           `json:"node_id"`
	Timestamp time.Time        `json:"timestamp"`
	Data      interface{}      `json:"data"`
	Message   string           `json:"message"`
}

// ElasticEventType 事件类型
type ElasticEventType string

const (
	EventScalingDecision ElasticEventType = "scaling_decision"
	EventStateTransition ElasticEventType = "state_transition"
	EventInstanceCreated ElasticEventType = "instance_created"
	EventInstanceDeleted ElasticEventType = "instance_deleted"
	EventError           ElasticEventType = "error"
)

// DefaultElasticControllerConfig 默认弹性控制器配置
func DefaultElasticControllerConfig() *ElasticControllerConfig {
	return &ElasticControllerConfig{
		BPScalerConfig:     DefaultBPScalerConfig(),
		DecisionInterval:   30 * time.Second,
		MonitoringInterval: 10 * time.Second,
		CloudProviders:     make(map[string]*CloudProviderConfig),
		DefaultInstanceConfig: &InstanceConfig{
			InstanceType: "t3.micro",
			Tags: map[string]string{
				"Project": "Rigel",
				"Type":    "ElasticNode",
			},
		},
		EventBufferSize: 1000,
	}
}

// NewElasticController 创建弹性控制器
func NewElasticController(config *ElasticControllerConfig) *ElasticController {
	if config == nil {
		config = DefaultElasticControllerConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	ec := &ElasticController{
		config:       config,
		bpScaler:     NewBPScaler(config.BPScalerConfig),
		stateManager: NewElasticStateManager(config.BPScalerConfig),
		cloudManager: NewCloudManager(),
		ctx:          ctx,
		cancel:       cancel,
		stats:        &ElasticControllerStats{},
		eventChan:    make(chan *ElasticEvent, config.EventBufferSize),
	}

	// 初始化云服务商
	for name, providerConfig := range config.CloudProviders {
		if err := ec.cloudManager.AddProvider(name, providerConfig); err != nil {
			// 记录错误但不阻止启动
			ec.emitEvent(EventError, "", fmt.Sprintf("Failed to add provider %s: %v", name, err))
		}
	}

	return ec
}

// Start 启动弹性控制器
func (ec *ElasticController) Start() error {
	// 启动决策循环
	ec.wg.Add(1)
	go ec.decisionLoop()

	// 启动监控循环
	ec.wg.Add(1)
	go ec.monitoringLoop()

	// 启动事件处理循环
	ec.wg.Add(1)
	go ec.eventLoop()

	return nil
}

// Stop 停止弹性控制器
func (ec *ElasticController) Stop() error {
	ec.cancel()
	ec.wg.Wait()

	// 清理资源
	ec.stateManager.Cleanup()
	close(ec.eventChan)

	return nil
}

// ProcessScalingRequest 处理伸缩请求
func (ec *ElasticController) ProcessScalingRequest(nodeID string, currentVQ float64) (*BPScalerDecision, error) {
	// 使用BP-Scaler做出决策
	decision, err := ec.bpScaler.MakeScalingDecision(nodeID, currentVQ)
	if err != nil {
		return nil, fmt.Errorf("failed to make scaling decision: %w", err)
	}

	// 发出决策事件
	ec.emitEvent(EventScalingDecision, nodeID, fmt.Sprintf("Decision: %t, Reason: %s", decision.ShouldScale, decision.Reason))

	// 如果需要伸缩，执行状态转换
	if decision.ShouldScale {
		if err := ec.executeScalingAction(decision); err != nil {
			ec.emitEvent(EventError, nodeID, fmt.Sprintf("Failed to execute scaling action: %v", err))
			return decision, err
		}
	}

	// 更新统计信息
	ec.updateStats(decision)

	return decision, nil
}

// executeScalingAction 执行伸缩动作
func (ec *ElasticController) executeScalingAction(decision *BPScalerDecision) error {
	// 执行状态转换
	err := ec.stateManager.TransitionState(decision.NodeID, decision.TargetState, decision.Reason)
	if err != nil {
		return fmt.Errorf("failed to transition state: %w", err)
	}

	// 发出状态转换事件
	ec.emitEvent(EventStateTransition, decision.NodeID,
		fmt.Sprintf("State transition: %s -> %s", decision.CurrentState, decision.TargetState))

	// 如果需要创建云实例
	if decision.TargetState == StateScalingUp {
		return ec.createCloudInstance(decision.NodeID)
	}

	// 如果需要释放云实例
	if decision.TargetState == StateReleasing {
		return ec.deleteCloudInstance(decision.NodeID)
	}

	return nil
}

// createCloudInstance 创建云实例
func (ec *ElasticController) createCloudInstance(nodeID string) error {
	// 选择云服务商（简化实现：使用第一个可用的）
	providerNames := ec.cloudManager.GetProviderNames()
	if len(providerNames) == 0 {
		return fmt.Errorf("no cloud providers configured")
	}

	providerName := providerNames[0]

	// 准备实例配置
	instanceConfig := *ec.config.DefaultInstanceConfig
	instanceConfig.Tags["NodeID"] = nodeID
	instanceConfig.Tags["CreatedAt"] = time.Now().Format(time.RFC3339)

	// 创建实例
	ctx, cancel := context.WithTimeout(ec.ctx, 5*time.Minute)
	defer cancel()

	instance, err := ec.cloudManager.CreateInstance(ctx, providerName, &instanceConfig)
	if err != nil {
		// 创建失败，转换到RELEASING状态
		ec.stateManager.TransitionState(nodeID, StateReleasing, fmt.Sprintf("Instance creation failed: %v", err))
		return fmt.Errorf("failed to create instance: %w", err)
	}

	// 创建成功，转换到DORMANT状态
	ec.stateManager.TransitionState(nodeID, StateDormant, "Instance created successfully")
	ec.emitEvent(EventInstanceCreated, nodeID, fmt.Sprintf("Instance %s created", instance.InstanceID))

	return nil
}

// deleteCloudInstance 删除云实例
func (ec *ElasticController) deleteCloudInstance(nodeID string) error {
	// 查找实例
	ctx, cancel := context.WithTimeout(ec.ctx, 2*time.Minute)
	defer cancel()

	allInstances, err := ec.cloudManager.GetAllInstances(ctx)
	if err != nil {
		return fmt.Errorf("failed to get instances: %w", err)
	}

	// 查找对应的实例
	var targetInstance *CloudInstance
	var targetProvider string

	for providerName, instances := range allInstances {
		for _, instance := range instances {
			if instance.NodeID == nodeID {
				targetInstance = instance
				targetProvider = providerName
				break
			}
		}
		if targetInstance != nil {
			break
		}
	}

	if targetInstance == nil {
		// 实例不存在，直接转换到INACTIVE状态
		ec.stateManager.TransitionState(nodeID, StateInactive, "Instance not found")
		return nil
	}

	// 删除实例
	err = ec.cloudManager.DeleteInstance(ctx, targetProvider, targetInstance.InstanceID)
	if err != nil {
		return fmt.Errorf("failed to delete instance: %w", err)
	}

	// 删除成功，转换到INACTIVE状态
	ec.stateManager.TransitionState(nodeID, StateInactive, "Instance deleted successfully")
	ec.emitEvent(EventInstanceDeleted, nodeID, fmt.Sprintf("Instance %s deleted", targetInstance.InstanceID))

	return nil
}

// decisionLoop 决策循环
func (ec *ElasticController) decisionLoop() {
	defer ec.wg.Done()

	ticker := time.NewTicker(ec.config.DecisionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ec.ctx.Done():
			return
		case <-ticker.C:
			// 这里可以添加定期决策逻辑
			// 例如：检查所有节点的状态，主动触发决策
		}
	}
}

// monitoringLoop 监控循环
func (ec *ElasticController) monitoringLoop() {
	defer ec.wg.Done()

	ticker := time.NewTicker(ec.config.MonitoringInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ec.ctx.Done():
			return
		case <-ticker.C:
			ec.updateSystemStats()
		}
	}
}

// eventLoop 事件处理循环
func (ec *ElasticController) eventLoop() {
	defer ec.wg.Done()

	for {
		select {
		case <-ec.ctx.Done():
			return
		case event := <-ec.eventChan:
			// 处理事件（例如：记录日志、发送通知等）
			ec.handleEvent(event)
		}
	}
}

// emitEvent 发出事件
func (ec *ElasticController) emitEvent(eventType ElasticEventType, nodeID, message string) {
	event := &ElasticEvent{
		Type:      eventType,
		NodeID:    nodeID,
		Timestamp: time.Now(),
		Message:   message,
	}

	select {
	case ec.eventChan <- event:
	default:
		// 事件缓冲区满，丢弃事件
	}
}

// handleEvent 处理事件
func (ec *ElasticController) handleEvent(event *ElasticEvent) {
	// 简化实现：只记录事件
	// 在实际实现中，可以添加日志记录、指标上报、通知发送等功能
	_ = event
}

// updateStats 更新统计信息
func (ec *ElasticController) updateStats(decision *BPScalerDecision) {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	ec.stats.TotalScalingDecisions++
	if decision.ShouldScale {
		ec.stats.SuccessfulScalings++
	}
}

// updateSystemStats 更新系统统计信息
func (ec *ElasticController) updateSystemStats() {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	// 获取状态统计
	stateStats := ec.stateManager.GetStateStatistics()
	ec.stats.ActiveNodes = stateStats[StateTriggered]
	ec.stats.DormantNodes = stateStats[StateDormant]
	ec.stats.ScalingUpNodes = stateStats[StateScalingUp]
	ec.stats.ReleasingNodes = stateStats[StateReleasing]
	ec.stats.PermanentNodes = stateStats[StatePermanent]
	ec.stats.TotalNodes = 0
	for _, count := range stateStats {
		ec.stats.TotalNodes += count
	}

	// 获取波动性统计
	ec.stats.AverageVolatility = ec.bpScaler.CalculateSystemVolatility()
	ec.stats.AveragePerturbation = ec.bpScaler.CalculateSystemPerturbation()
}

// GetStats 获取统计信息
func (ec *ElasticController) GetStats() *ElasticControllerStats {
	ec.mu.RLock()
	defer ec.mu.RUnlock()

	statsCopy := *ec.stats
	return &statsCopy
}

// GetNodeState 获取节点状态
func (ec *ElasticController) GetNodeState(nodeID string) (*ElasticNodeInfo, bool) {
	return ec.stateManager.GetNodeState(nodeID)
}

// GetAllNodeStates 获取所有节点状态
func (ec *ElasticController) GetAllNodeStates() map[string]*ElasticNodeInfo {
	return ec.stateManager.GetAllNodeStates()
}

// GetDecisionHistory 获取决策历史
func (ec *ElasticController) GetDecisionHistory(nodeID string) ([]*BPScalerDecision, bool) {
	return ec.bpScaler.GetDecisionHistory(nodeID)
}

// GetTransitionHistory 获取状态转换历史
func (ec *ElasticController) GetTransitionHistory(nodeID string) ([]*StateTransition, bool) {
	return ec.stateManager.GetTransitionHistory(nodeID)
}

// ForceScaling 强制触发伸缩（用于测试和管理）
func (ec *ElasticController) ForceScaling(nodeID string, targetState ElasticNodeState, reason string) error {
	// 强制状态转换
	err := ec.stateManager.ForceTransition(nodeID, targetState, reason)
	if err != nil {
		return fmt.Errorf("failed to force transition: %w", err)
	}

	// 发出事件
	ec.emitEvent(EventStateTransition, nodeID, fmt.Sprintf("FORCED transition to %s: %s", targetState, reason))

	// 执行相应的云操作
	if targetState == StateScalingUp {
		return ec.createCloudInstance(nodeID)
	} else if targetState == StateReleasing {
		return ec.deleteCloudInstance(nodeID)
	}

	return nil
}

// SimulateDecision 模拟决策（不执行实际操作）
func (ec *ElasticController) SimulateDecision(nodeID string, currentVQ float64) (*BPScalerDecision, error) {
	return ec.bpScaler.SimulateDecision(nodeID, currentVQ)
}

// UpdateNodeCosts 更新节点成本参数
func (ec *ElasticController) UpdateNodeCosts(nodeID string, fixedCost, variableCost float64) error {
	// 更新BP-Scaler中的成本
	if err := ec.bpScaler.UpdateNodeCosts(nodeID, fixedCost, variableCost); err != nil {
		return err
	}

	// 更新状态管理器中的成本
	return ec.stateManager.UpdateNodeCosts(nodeID, fixedCost, variableCost)
}

// GetCloudInstances 获取所有云实例
func (ec *ElasticController) GetCloudInstances(ctx context.Context) (map[string][]*CloudInstance, error) {
	return ec.cloudManager.GetAllInstances(ctx)
}

// AddCloudProvider 添加云服务商
func (ec *ElasticController) AddCloudProvider(name string, config *CloudProviderConfig) error {
	return ec.cloudManager.AddProvider(name, config)
}

// RemoveCloudProvider 移除云服务商
func (ec *ElasticController) RemoveCloudProvider(name string) error {
	return ec.cloudManager.RemoveProvider(name)
}

// GetActiveNodes 获取活跃节点
func (ec *ElasticController) GetActiveNodes() map[string]*ElasticNodeInfo {
	return ec.stateManager.GetActiveNodes()
}

// GetScalableNodes 获取可伸缩节点
func (ec *ElasticController) GetScalableNodes() map[string]*ElasticNodeInfo {
	return ec.stateManager.GetScalableNodes()
}

// GetStateStatistics 获取状态统计
func (ec *ElasticController) GetStateStatistics() map[ElasticNodeState]int {
	return ec.stateManager.GetStateStatistics()
}

// ValidateConfig 验证配置
func (ec *ElasticController) ValidateConfig() error {
	if ec.config == nil {
		return fmt.Errorf("elastic controller config cannot be nil")
	}

	if ec.config.DecisionInterval <= 0 {
		return fmt.Errorf("decision interval must be positive")
	}

	if ec.config.MonitoringInterval <= 0 {
		return fmt.Errorf("monitoring interval must be positive")
	}

	// 验证BP-Scaler配置
	return ec.bpScaler.ValidateConfig()
}

// GetEventChannel 获取事件通道（只读）
func (ec *ElasticController) GetEventChannel() <-chan *ElasticEvent {
	return ec.eventChan
}

// Reset 重置控制器状态（用于测试）
func (ec *ElasticController) Reset() {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	ec.bpScaler.Reset()
	ec.stateManager.Reset()
	ec.stats = &ElasticControllerStats{}

	// 清空事件通道
	for len(ec.eventChan) > 0 {
		<-ec.eventChan
	}
}
