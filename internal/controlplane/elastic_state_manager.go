package controlplane

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// ElasticStateManager 弹性节点状态机管理器
// 实现论文中 Figure 8 所示的弹性资源状态转换图
type ElasticStateManager struct {
	config *BPScalerConfig

	// 节点状态存储
	nodeStates map[string]*ElasticNodeInfo
	mu         sync.RWMutex

	// 状态转换历史
	transitionHistory map[string][]*StateTransition
	maxHistorySize    int

	// 定时器管理
	retainTimers map[string]*time.Timer
	timerMu      sync.Mutex
}

// NewElasticStateManager 创建新的状态管理器
func NewElasticStateManager(config *BPScalerConfig) *ElasticStateManager {
	if config == nil {
		config = DefaultBPScalerConfig()
	}

	return &ElasticStateManager{
		config:            config,
		nodeStates:        make(map[string]*ElasticNodeInfo),
		transitionHistory: make(map[string][]*StateTransition),
		maxHistorySize:    100,
		retainTimers:      make(map[string]*time.Timer),
	}
}

// TransitionState 执行状态转换
func (esm *ElasticStateManager) TransitionState(nodeID string, targetState ElasticNodeState, reason string) error {
	esm.mu.Lock()
	defer esm.mu.Unlock()

	nodeInfo, exists := esm.nodeStates[nodeID]
	if !exists {
		// 创建新节点
		nodeInfo = &ElasticNodeInfo{
			NodeID:       nodeID,
			State:        StateInactive,
			CreatedAt:    time.Now(),
			FixedCost:    100.0,
			VariableCost: 1.0,
			StateHistory: make([]StateTransition, 0),
		}
		esm.nodeStates[nodeID] = nodeInfo
	}

	currentState := nodeInfo.State

	// 验证状态转换是否合法
	if !esm.isValidTransition(currentState, targetState) {
		return fmt.Errorf("invalid state transition from %s to %s for node %s",
			currentState, targetState, nodeID)
	}

	// 执行状态转换
	return esm.executeTransition(nodeInfo, targetState, reason)
}

// isValidTransition 检查状态转换是否合法
func (esm *ElasticStateManager) isValidTransition(from, to ElasticNodeState) bool {
	validTransitions := map[ElasticNodeState][]ElasticNodeState{
		StateInactive:  {StateScalingUp},
		StateScalingUp: {StateDormant, StateReleasing}, // 启动成功或失败
		StateDormant:   {StateTriggered, StateReleasing},
		StateTriggered: {StateDormant, StatePermanent},
		StatePermanent: {StateReleasing},
		StateReleasing: {StateInactive},
	}

	allowedStates, exists := validTransitions[from]
	if !exists {
		return false
	}

	for _, allowedState := range allowedStates {
		if allowedState == to {
			return true
		}
	}

	return false
}

// executeTransition 执行状态转换
func (esm *ElasticStateManager) executeTransition(nodeInfo *ElasticNodeInfo, targetState ElasticNodeState, reason string) error {
	now := time.Now()
	fromState := nodeInfo.State

	// 记录状态转换
	transition := StateTransition{
		FromState: fromState,
		ToState:   targetState,
		Timestamp: now,
		Reason:    reason,
	}

	// 更新节点状态
	nodeInfo.State = targetState
	nodeInfo.StateHistory = append(nodeInfo.StateHistory, transition)

	// 限制历史记录大小
	if len(nodeInfo.StateHistory) > esm.maxHistorySize {
		nodeInfo.StateHistory = nodeInfo.StateHistory[1:]
	}

	// 处理特定状态转换的逻辑
	switch targetState {
	case StateTriggered:
		nodeInfo.LastTriggeredAt = now
		esm.updateActivityScore(nodeInfo)
		esm.cancelRetainTimer(nodeInfo.NodeID)

	case StateDormant:
		if fromState == StateTriggered {
			// 从激活状态回到休眠，开始计算保留时间
			esm.calculateAndSetRetainTime(nodeInfo)
			esm.startRetainTimer(nodeInfo)
		}

	case StatePermanent:
		// 进入永久状态，取消保留定时器
		esm.cancelRetainTimer(nodeInfo.NodeID)

	case StateReleasing:
		// 开始释放，取消保留定时器
		esm.cancelRetainTimer(nodeInfo.NodeID)
	}

	// 保存转换历史
	esm.saveTransitionHistory(nodeInfo.NodeID, &transition)

	return nil
}

// updateActivityScore 更新活动分数
// 实现论文中的指数衰减加权和计算
func (esm *ElasticStateManager) updateActivityScore(nodeInfo *ElasticNodeInfo) {
	now := time.Now()

	// 如果是第一次触发
	if nodeInfo.LastTriggeredAt.IsZero() {
		nodeInfo.ActivityScore = 1.0
		return
	}

	// 计算时间间隔（小时）
	timeDiff := now.Sub(nodeInfo.LastTriggeredAt).Hours()

	// 指数衰减：activity_score = decay_rate^timeDiff * old_score + 1
	decayFactor := math.Pow(esm.config.ActivityDecayRate, timeDiff)
	nodeInfo.ActivityScore = decayFactor*nodeInfo.ActivityScore + 1.0
}

// calculateAndSetRetainTime 计算并设置保留时间
// 实现论文公式 (22, 23)
func (esm *ElasticStateManager) calculateAndSetRetainTime(nodeInfo *ElasticNodeInfo) {
	// ri(t) = T0 * (1 + activity_score)
	retainTime := esm.config.BaseRetainTime * (1.0 + nodeInfo.ActivityScore)

	// 检查是否应该进入永久状态
	if retainTime > esm.config.PermanentThreshold {
		nodeInfo.RetainTime = esm.config.PermanentThreshold
	} else {
		nodeInfo.RetainTime = retainTime
	}
}

// startRetainTimer 启动保留定时器
func (esm *ElasticStateManager) startRetainTimer(nodeInfo *ElasticNodeInfo) {
	esm.timerMu.Lock()
	defer esm.timerMu.Unlock()

	// 取消现有定时器
	if timer, exists := esm.retainTimers[nodeInfo.NodeID]; exists {
		timer.Stop()
	}

	// 创建新定时器
	duration := time.Duration(nodeInfo.RetainTime) * time.Second
	timer := time.AfterFunc(duration, func() {
		esm.handleRetainTimeout(nodeInfo.NodeID)
	})

	esm.retainTimers[nodeInfo.NodeID] = timer
}

// cancelRetainTimer 取消保留定时器
func (esm *ElasticStateManager) cancelRetainTimer(nodeID string) {
	esm.timerMu.Lock()
	defer esm.timerMu.Unlock()

	if timer, exists := esm.retainTimers[nodeID]; exists {
		timer.Stop()
		delete(esm.retainTimers, nodeID)
	}
}

// handleRetainTimeout 处理保留时间超时
func (esm *ElasticStateManager) handleRetainTimeout(nodeID string) {
	esm.mu.Lock()
	defer esm.mu.Unlock()

	nodeInfo, exists := esm.nodeStates[nodeID]
	if !exists {
		return
	}

	// 只有在DORMANT状态下才处理超时
	if nodeInfo.State == StateDormant {
		// 检查是否应该进入永久状态
		if nodeInfo.RetainTime >= esm.config.PermanentThreshold {
			esm.executeTransition(nodeInfo, StatePermanent, "Retain time exceeded permanent threshold")
		} else {
			esm.executeTransition(nodeInfo, StateReleasing, "Retain time expired")
		}
	}
}

// saveTransitionHistory 保存状态转换历史
func (esm *ElasticStateManager) saveTransitionHistory(nodeID string, transition *StateTransition) {
	history, exists := esm.transitionHistory[nodeID]
	if !exists {
		history = make([]*StateTransition, 0, esm.maxHistorySize)
	}

	// 创建副本
	transitionCopy := *transition
	history = append(history, &transitionCopy)

	// 限制历史大小
	if len(history) > esm.maxHistorySize {
		history = history[1:]
	}

	esm.transitionHistory[nodeID] = history
}

// GetNodeState 获取节点状态
func (esm *ElasticStateManager) GetNodeState(nodeID string) (*ElasticNodeInfo, bool) {
	esm.mu.RLock()
	defer esm.mu.RUnlock()

	nodeInfo, exists := esm.nodeStates[nodeID]
	if !exists {
		return nil, false
	}

	// 返回副本
	nodeInfoCopy := *nodeInfo
	return &nodeInfoCopy, true
}

// GetAllNodeStates 获取所有节点状态
func (esm *ElasticStateManager) GetAllNodeStates() map[string]*ElasticNodeInfo {
	esm.mu.RLock()
	defer esm.mu.RUnlock()

	result := make(map[string]*ElasticNodeInfo)
	for nodeID, nodeInfo := range esm.nodeStates {
		nodeInfoCopy := *nodeInfo
		result[nodeID] = &nodeInfoCopy
	}

	return result
}

// GetTransitionHistory 获取状态转换历史
func (esm *ElasticStateManager) GetTransitionHistory(nodeID string) ([]*StateTransition, bool) {
	esm.mu.RLock()
	defer esm.mu.RUnlock()

	history, exists := esm.transitionHistory[nodeID]
	if !exists {
		return nil, false
	}

	// 返回副本
	result := make([]*StateTransition, len(history))
	for i, transition := range history {
		transitionCopy := *transition
		result[i] = &transitionCopy
	}

	return result, true
}

// GetStateStatistics 获取状态统计信息
func (esm *ElasticStateManager) GetStateStatistics() map[ElasticNodeState]int {
	esm.mu.RLock()
	defer esm.mu.RUnlock()

	stats := make(map[ElasticNodeState]int)

	// 初始化所有状态计数
	for _, state := range []ElasticNodeState{
		StateInactive, StateScalingUp, StateReleasing,
		StateDormant, StateTriggered, StatePermanent,
	} {
		stats[state] = 0
	}

	// 统计各状态节点数量
	for _, nodeInfo := range esm.nodeStates {
		stats[nodeInfo.State]++
	}

	return stats
}

// Cleanup 清理资源
func (esm *ElasticStateManager) Cleanup() {
	esm.timerMu.Lock()
	defer esm.timerMu.Unlock()

	// 停止所有定时器
	for _, timer := range esm.retainTimers {
		timer.Stop()
	}
	esm.retainTimers = make(map[string]*time.Timer)
}

// ForceTransition 强制状态转换（用于测试和管理）
func (esm *ElasticStateManager) ForceTransition(nodeID string, targetState ElasticNodeState, reason string) error {
	esm.mu.Lock()
	defer esm.mu.Unlock()

	nodeInfo, exists := esm.nodeStates[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	return esm.executeTransition(nodeInfo, targetState, fmt.Sprintf("FORCED: %s", reason))
}

// UpdateNodeCosts 更新节点成本参数
func (esm *ElasticStateManager) UpdateNodeCosts(nodeID string, fixedCost, variableCost float64) error {
	esm.mu.Lock()
	defer esm.mu.Unlock()

	nodeInfo, exists := esm.nodeStates[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	nodeInfo.FixedCost = fixedCost
	nodeInfo.VariableCost = variableCost

	return nil
}

// GetActiveNodes 获取所有活跃节点（TRIGGERED和PERMANENT状态）
func (esm *ElasticStateManager) GetActiveNodes() map[string]*ElasticNodeInfo {
	esm.mu.RLock()
	defer esm.mu.RUnlock()

	result := make(map[string]*ElasticNodeInfo)
	for nodeID, nodeInfo := range esm.nodeStates {
		if nodeInfo.State == StateTriggered || nodeInfo.State == StatePermanent {
			nodeInfoCopy := *nodeInfo
			result[nodeID] = &nodeInfoCopy
		}
	}

	return result
}

// GetScalableNodes 获取可伸缩节点（INACTIVE和DORMANT状态）
func (esm *ElasticStateManager) GetScalableNodes() map[string]*ElasticNodeInfo {
	esm.mu.RLock()
	defer esm.mu.RUnlock()

	result := make(map[string]*ElasticNodeInfo)
	for nodeID, nodeInfo := range esm.nodeStates {
		if nodeInfo.State == StateInactive || nodeInfo.State == StateDormant {
			nodeInfoCopy := *nodeInfo
			result[nodeID] = &nodeInfoCopy
		}
	}

	return result
}

// CalculateRetainTime 计算节点的保留时间（不更新状态）
func (esm *ElasticStateManager) CalculateRetainTime(nodeID string) (float64, error) {
	esm.mu.RLock()
	defer esm.mu.RUnlock()

	nodeInfo, exists := esm.nodeStates[nodeID]
	if !exists {
		return 0, fmt.Errorf("node %s not found", nodeID)
	}

	retainTime := esm.config.BaseRetainTime * (1.0 + nodeInfo.ActivityScore)
	if retainTime > esm.config.PermanentThreshold {
		return esm.config.PermanentThreshold, nil
	}

	return retainTime, nil
}

// GetRetainTimeRemaining 获取节点剩余保留时间
func (esm *ElasticStateManager) GetRetainTimeRemaining(nodeID string) (time.Duration, error) {
	esm.mu.RLock()
	nodeInfo, exists := esm.nodeStates[nodeID]
	esm.mu.RUnlock()

	if !exists {
		return 0, fmt.Errorf("node %s not found", nodeID)
	}

	if nodeInfo.State != StateDormant {
		return 0, fmt.Errorf("node %s is not in DORMANT state", nodeID)
	}

	esm.timerMu.Lock()
	defer esm.timerMu.Unlock()

	_, exists = esm.retainTimers[nodeID]
	if !exists {
		return 0, fmt.Errorf("no retain timer found for node %s", nodeID)
	}

	// 这是一个近似值，因为Go的Timer不提供直接获取剩余时间的方法
	// 在实际实现中，可能需要记录定时器开始时间来精确计算
	totalRetainTime := time.Duration(nodeInfo.RetainTime) * time.Second
	elapsed := time.Since(nodeInfo.LastTriggeredAt)
	remaining := totalRetainTime - elapsed

	if remaining < 0 {
		return 0, nil
	}

	return remaining, nil
}

// Reset 重置状态管理器
func (esm *ElasticStateManager) Reset() {
	esm.mu.Lock()
	defer esm.mu.Unlock()

	// 清理定时器
	esm.Cleanup()

	// 重置状态
	esm.nodeStates = make(map[string]*ElasticNodeInfo)
	esm.transitionHistory = make(map[string][]*StateTransition)
}
