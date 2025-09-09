package controlplane

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// BPScaler 基于反压的弹性伸缩器
// 实现论文中的 Algorithm 5.1 (BP-Scaler)
type BPScaler struct {
	config *BPScalerConfig

	// 波动性计算器
	volatilityCalc *VolatilityCalculator

	// 节点状态管理
	nodeStates map[string]*ElasticNodeInfo
	mu         sync.RWMutex

	// 决策历史
	decisionHistory map[string][]*BPScalerDecision
	maxHistorySize  int

	// 统计信息
	stats *BPScalerStats
}

// BPScalerStats BP-Scaler统计信息
type BPScalerStats struct {
	TotalDecisions     int       `json:"total_decisions"`
	PositiveDecisions  int       `json:"positive_decisions"`
	NegativeDecisions  int       `json:"negative_decisions"`
	AverageDecisionVar float64   `json:"average_decision_var"`
	TotalCost          float64   `json:"total_cost"`
	LastDecisionTime   time.Time `json:"last_decision_time"`
}

// NewBPScaler 创建新的BP-Scaler
func NewBPScaler(config *BPScalerConfig) *BPScaler {
	if config == nil {
		config = DefaultBPScalerConfig()
	}

	return &BPScaler{
		config:          config,
		volatilityCalc:  NewVolatilityCalculator(config),
		nodeStates:      make(map[string]*ElasticNodeInfo),
		decisionHistory: make(map[string][]*BPScalerDecision),
		maxHistorySize:  100,
		stats:           &BPScalerStats{},
	}
}

// MakeScalingDecision 执行伸缩决策
// 实现论文中的 Algorithm 5.1 (BP-Scaler)
func (bp *BPScaler) MakeScalingDecision(nodeID string, currentVQ float64) (*BPScalerDecision, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	now := time.Now()

	// 1. 更新波动性指标
	volatilityMetrics, err := bp.volatilityCalc.UpdateVolatilityMetrics(nodeID, currentVQ)
	if err != nil {
		return nil, fmt.Errorf("failed to update volatility metrics: %w", err)
	}

	// 2. 获取或创建节点状态
	nodeInfo, exists := bp.nodeStates[nodeID]
	if !exists {
		nodeInfo = &ElasticNodeInfo{
			NodeID:       nodeID,
			State:        StateInactive,
			CreatedAt:    now,
			FixedCost:    100.0, // 默认固定成本
			VariableCost: 1.0,   // 默认变量成本
			StateHistory: make([]StateTransition, 0),
		}
		bp.nodeStates[nodeID] = nodeInfo
	}

	// 3. 计算伸缩成本 Cost_i(t,s)
	scalingCost := bp.calculateScalingCost(nodeInfo, volatilityMetrics.Perturbation)

	// 4. 计算决策变量 Δi(t)
	// Δi(t) = -γi*w_i^S*Zi(t)*Pi(t) + V_S*Cost_i(t,s)
	decisionVar := bp.calculateDecisionVariable(volatilityMetrics, scalingCost)

	// 5. 做出决策
	shouldScale := decisionVar > 0
	targetState := bp.determineTargetState(nodeInfo.State, shouldScale)

	// 6. 创建决策结果
	decision := &BPScalerDecision{
		NodeID:          nodeID,
		ShouldScale:     shouldScale,
		DecisionVar:     decisionVar,
		ScalingCost:     scalingCost,
		CurrentState:    nodeInfo.State,
		TargetState:     targetState,
		Timestamp:       now,
		VolatilityQueue: volatilityMetrics.VolatilityQueue,
		Perturbation:    volatilityMetrics.Perturbation,
		ScalingWeight:   bp.config.ScalingWeight,
		CostWeight:      bp.config.CostWeight,
	}

	// 7. 设置决策原因
	decision.Reason = bp.generateDecisionReason(decision, volatilityMetrics)

	// 8. 保存决策历史
	bp.saveDecisionHistory(nodeID, decision)

	// 9. 更新统计信息
	bp.updateStats(decision)

	return decision, nil
}

// calculateScalingCost 计算伸缩成本
// 根据论文公式 (19) 实现
func (bp *BPScaler) calculateScalingCost(nodeInfo *ElasticNodeInfo, perturbation float64) float64 {
	switch nodeInfo.State {
	case StateInactive:
		// INITIATE: 包含固定启动成本
		return nodeInfo.FixedCost + nodeInfo.VariableCost*perturbation
	case StateDormant:
		// DORMANT: 只有变量成本
		return nodeInfo.VariableCost * perturbation
	default:
		// 其他状态不需要伸缩成本
		return 0.0
	}
}

// calculateDecisionVariable 计算决策变量
// 实现论文中的决策变量计算：Δi(t) = -γi*w_i^S*Zi(t)*Pi(t) + V_S*Cost_i(t,s)
func (bp *BPScaler) calculateDecisionVariable(metrics *VolatilityMetrics, scalingCost float64) float64 {
	// -γi*w_i^S*Zi(t)*Pi(t): 负的加权波动性项
	volatilityTerm := -metrics.DecayFactor * bp.config.ScalingWeight *
		metrics.VolatilityQueue * metrics.Perturbation

	// V_S*Cost_i(t,s): 加权成本项
	costTerm := bp.config.CostWeight * scalingCost

	return volatilityTerm + costTerm
}

// determineTargetState 确定目标状态
func (bp *BPScaler) determineTargetState(currentState ElasticNodeState, shouldScale bool) ElasticNodeState {
	if !shouldScale {
		return currentState
	}

	switch currentState {
	case StateInactive:
		return StateScalingUp
	case StateDormant:
		return StateTriggered
	default:
		return currentState
	}
}

// generateDecisionReason 生成决策原因
func (bp *BPScaler) generateDecisionReason(decision *BPScalerDecision, metrics *VolatilityMetrics) string {
	if decision.ShouldScale {
		return fmt.Sprintf("High volatility detected: Zi(t)=%.3f, Pi(t)=%.3f, DecisionVar=%.3f > 0",
			metrics.VolatilityQueue, metrics.Perturbation, decision.DecisionVar)
	} else {
		return fmt.Sprintf("Low volatility: Zi(t)=%.3f, Pi(t)=%.3f, DecisionVar=%.3f <= 0",
			metrics.VolatilityQueue, metrics.Perturbation, decision.DecisionVar)
	}
}

// saveDecisionHistory 保存决策历史
func (bp *BPScaler) saveDecisionHistory(nodeID string, decision *BPScalerDecision) {
	history, exists := bp.decisionHistory[nodeID]
	if !exists {
		history = make([]*BPScalerDecision, 0, bp.maxHistorySize)
	}

	// 创建副本
	decisionCopy := *decision
	history = append(history, &decisionCopy)

	// 限制历史大小
	if len(history) > bp.maxHistorySize {
		history = history[1:]
	}

	bp.decisionHistory[nodeID] = history
}

// updateStats 更新统计信息
func (bp *BPScaler) updateStats(decision *BPScalerDecision) {
	bp.stats.TotalDecisions++
	bp.stats.LastDecisionTime = decision.Timestamp
	bp.stats.TotalCost += decision.ScalingCost

	if decision.ShouldScale {
		bp.stats.PositiveDecisions++
	} else {
		bp.stats.NegativeDecisions++
	}

	// 更新平均决策变量
	if bp.stats.TotalDecisions > 0 {
		bp.stats.AverageDecisionVar = (bp.stats.AverageDecisionVar*float64(bp.stats.TotalDecisions-1) +
			decision.DecisionVar) / float64(bp.stats.TotalDecisions)
	}
}

// GetNodeState 获取节点状态
func (bp *BPScaler) GetNodeState(nodeID string) (*ElasticNodeInfo, bool) {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	nodeInfo, exists := bp.nodeStates[nodeID]
	if !exists {
		return nil, false
	}

	// 返回副本
	nodeInfoCopy := *nodeInfo
	return &nodeInfoCopy, true
}

// GetDecisionHistory 获取决策历史
func (bp *BPScaler) GetDecisionHistory(nodeID string) ([]*BPScalerDecision, bool) {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	history, exists := bp.decisionHistory[nodeID]
	if !exists {
		return nil, false
	}

	// 返回副本
	result := make([]*BPScalerDecision, len(history))
	for i, decision := range history {
		decisionCopy := *decision
		result[i] = &decisionCopy
	}

	return result, true
}

// GetStats 获取统计信息
func (bp *BPScaler) GetStats() *BPScalerStats {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	statsCopy := *bp.stats
	return &statsCopy
}

// Reset 重置BP-Scaler状态
func (bp *BPScaler) Reset() {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	bp.nodeStates = make(map[string]*ElasticNodeInfo)
	bp.decisionHistory = make(map[string][]*BPScalerDecision)
	bp.stats = &BPScalerStats{}
	bp.volatilityCalc.Reset()
}

// UpdateNodeCosts 更新节点成本参数
func (bp *BPScaler) UpdateNodeCosts(nodeID string, fixedCost, variableCost float64) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	nodeInfo, exists := bp.nodeStates[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	nodeInfo.FixedCost = fixedCost
	nodeInfo.VariableCost = variableCost

	return nil
}

// GetAllNodeStates 获取所有节点状态
func (bp *BPScaler) GetAllNodeStates() map[string]*ElasticNodeInfo {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	result := make(map[string]*ElasticNodeInfo)
	for nodeID, nodeInfo := range bp.nodeStates {
		nodeInfoCopy := *nodeInfo
		result[nodeID] = &nodeInfoCopy
	}

	return result
}

// CalculateSystemVolatility 计算系统整体波动性
func (bp *BPScaler) CalculateSystemVolatility() float64 {
	return bp.volatilityCalc.CalculateAverageVolatility()
}

// CalculateSystemPerturbation 计算系统整体扰动
func (bp *BPScaler) CalculateSystemPerturbation() float64 {
	return bp.volatilityCalc.CalculateAveragePerturbation()
}

// ValidateConfig 验证配置参数
func (bp *BPScaler) ValidateConfig() error {
	if bp.config == nil {
		return fmt.Errorf("BP-Scaler config cannot be nil")
	}

	if bp.config.ScalingWeight < 0 {
		return fmt.Errorf("scaling weight must be non-negative, got: %f", bp.config.ScalingWeight)
	}

	if bp.config.CostWeight < 0 {
		return fmt.Errorf("cost weight must be non-negative, got: %f", bp.config.CostWeight)
	}

	// 验证波动性计算器配置
	return bp.volatilityCalc.ValidateConfig()
}

// SimulateDecision 模拟决策（不更新状态）
func (bp *BPScaler) SimulateDecision(nodeID string, currentVQ float64) (*BPScalerDecision, error) {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	// 获取当前波动性指标（不更新）
	volatilityMetrics, exists := bp.volatilityCalc.GetVolatilityMetrics(nodeID)
	if !exists {
		// 如果节点不存在，创建临时指标
		volatilityMetrics = &VolatilityMetrics{
			NodeID:              nodeID,
			VirtualQueue:        currentVQ,
			Perturbation:        math.Max(0, currentVQ-bp.config.CongestionThreshold),
			VolatilityQueue:     0.0,
			PreviousVQ:          currentVQ,
			Timestamp:           time.Now(),
			CongestionThreshold: bp.config.CongestionThreshold,
			DecayFactor:         bp.config.DecayFactor,
			SensitivityWeight:   bp.config.SensitivityWeight,
			StabilityCapacity:   bp.config.StabilityCapacity,
		}
	}

	// 获取节点状态
	nodeInfo, exists := bp.nodeStates[nodeID]
	if !exists {
		nodeInfo = &ElasticNodeInfo{
			NodeID:       nodeID,
			State:        StateInactive,
			FixedCost:    100.0,
			VariableCost: 1.0,
		}
	}

	// 计算成本和决策变量
	scalingCost := bp.calculateScalingCost(nodeInfo, volatilityMetrics.Perturbation)
	decisionVar := bp.calculateDecisionVariable(volatilityMetrics, scalingCost)
	shouldScale := decisionVar > 0
	targetState := bp.determineTargetState(nodeInfo.State, shouldScale)

	decision := &BPScalerDecision{
		NodeID:          nodeID,
		ShouldScale:     shouldScale,
		DecisionVar:     decisionVar,
		ScalingCost:     scalingCost,
		CurrentState:    nodeInfo.State,
		TargetState:     targetState,
		Timestamp:       time.Now(),
		VolatilityQueue: volatilityMetrics.VolatilityQueue,
		Perturbation:    volatilityMetrics.Perturbation,
		ScalingWeight:   bp.config.ScalingWeight,
		CostWeight:      bp.config.CostWeight,
	}

	decision.Reason = bp.generateDecisionReason(decision, volatilityMetrics)

	return decision, nil
}
