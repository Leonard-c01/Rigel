package controlplane

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// VolatilityCalculator 波动性队列计算器
// 实现论文中的波动性队列 Zi(t) 计算逻辑
type VolatilityCalculator struct {
	config *BPScalerConfig

	// 节点波动性指标存储
	metrics map[string]*VolatilityMetrics
	mu      sync.RWMutex

	// 历史数据存储（用于调试和分析）
	history        map[string][]*VolatilityMetrics
	maxHistorySize int
}

// NewVolatilityCalculator 创建新的波动性队列计算器
func NewVolatilityCalculator(config *BPScalerConfig) *VolatilityCalculator {
	if config == nil {
		config = DefaultBPScalerConfig()
	}

	return &VolatilityCalculator{
		config:         config,
		metrics:        make(map[string]*VolatilityMetrics),
		history:        make(map[string][]*VolatilityMetrics),
		maxHistorySize: 100, // 保留最近100个时隙的历史数据
	}
}

// UpdateVolatilityMetrics 更新节点的波动性指标
// 根据论文公式 (16) 和 (17) 计算波动性队列
func (vc *VolatilityCalculator) UpdateVolatilityMetrics(nodeID string, currentVQ float64) (*VolatilityMetrics, error) {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	now := time.Now()

	// 获取或创建节点的波动性指标
	metrics, exists := vc.metrics[nodeID]
	if !exists {
		metrics = &VolatilityMetrics{
			NodeID:              nodeID,
			VirtualQueue:        currentVQ,
			Perturbation:        0.0,
			VolatilityQueue:     0.0,
			PreviousVQ:          currentVQ,
			Timestamp:           now,
			CongestionThreshold: vc.config.CongestionThreshold,
			DecayFactor:         vc.config.DecayFactor,
			SensitivityWeight:   vc.config.SensitivityWeight,
			StabilityCapacity:   vc.config.StabilityCapacity,
		}
		vc.metrics[nodeID] = metrics
		return metrics, nil
	}

	// 计算单时隙扰动 Pi(t) - 公式 (16)
	perturbation := vc.calculatePerturbation(metrics.VirtualQueue, currentVQ)

	// 更新波动性队列 Zi(t) - 公式 (17)
	newVolatilityQueue := vc.calculateVolatilityQueue(metrics.VolatilityQueue, perturbation)

	// 保存历史数据
	vc.saveToHistory(nodeID, metrics)

	// 更新指标
	metrics.PreviousVQ = metrics.VirtualQueue
	metrics.VirtualQueue = currentVQ
	metrics.Perturbation = perturbation
	metrics.VolatilityQueue = newVolatilityQueue
	metrics.Timestamp = now

	return metrics, nil
}

// calculatePerturbation 计算单时隙扰动 Pi(t)
// 公式 (16): Pi(t) = max[0, Õi(t) - CV]
func (vc *VolatilityCalculator) calculatePerturbation(previousVQ, currentVQ float64) float64 {
	// 计算虚拟队列的增量
	increment := currentVQ - previousVQ

	// 如果当前虚拟队列超过拥塞阈值，则计算扰动
	if currentVQ > vc.config.CongestionThreshold {
		perturbation := math.Max(0, increment)
		return perturbation
	}

	return 0.0
}

// calculateVolatilityQueue 计算波动性队列 Zi(t)
// 公式 (17): Zi(t+1) = max[0, γi*Zi(t) + w_i^V*Pi(t) - Ci(t)]
func (vc *VolatilityCalculator) calculateVolatilityQueue(currentZi, perturbation float64) float64 {
	// γi*Zi(t): 衰减的历史波动性
	decayedVolatility := vc.config.DecayFactor * currentZi

	// w_i^V*Pi(t): 加权的当前扰动
	weightedPerturbation := vc.config.SensitivityWeight * perturbation

	// Ci(t): 稳定能力（简化为常数）
	stabilityCapacity := vc.config.StabilityCapacity

	// 计算新的波动性队列值
	newZi := math.Max(0, decayedVolatility+weightedPerturbation-stabilityCapacity)

	return newZi
}

// GetVolatilityMetrics 获取节点的波动性指标
func (vc *VolatilityCalculator) GetVolatilityMetrics(nodeID string) (*VolatilityMetrics, bool) {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	metrics, exists := vc.metrics[nodeID]
	if !exists {
		return nil, false
	}

	// 返回副本以避免并发修改
	metricsCopy := *metrics
	return &metricsCopy, true
}

// GetAllVolatilityMetrics 获取所有节点的波动性指标
func (vc *VolatilityCalculator) GetAllVolatilityMetrics() map[string]*VolatilityMetrics {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	result := make(map[string]*VolatilityMetrics)
	for nodeID, metrics := range vc.metrics {
		metricsCopy := *metrics
		result[nodeID] = &metricsCopy
	}

	return result
}

// GetNodeHistory 获取节点的历史波动性数据
func (vc *VolatilityCalculator) GetNodeHistory(nodeID string) ([]*VolatilityMetrics, bool) {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	history, exists := vc.history[nodeID]
	if !exists {
		return nil, false
	}

	// 返回副本
	result := make([]*VolatilityMetrics, len(history))
	for i, metrics := range history {
		metricsCopy := *metrics
		result[i] = &metricsCopy
	}

	return result, true
}

// saveToHistory 保存历史数据
func (vc *VolatilityCalculator) saveToHistory(nodeID string, metrics *VolatilityMetrics) {
	history, exists := vc.history[nodeID]
	if !exists {
		history = make([]*VolatilityMetrics, 0, vc.maxHistorySize)
	}

	// 创建副本
	metricsCopy := *metrics
	history = append(history, &metricsCopy)

	// 限制历史数据大小
	if len(history) > vc.maxHistorySize {
		history = history[1:]
	}

	vc.history[nodeID] = history
}

// CalculateAverageVolatility 计算平均波动性
func (vc *VolatilityCalculator) CalculateAverageVolatility() float64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	if len(vc.metrics) == 0 {
		return 0.0
	}

	total := 0.0
	for _, metrics := range vc.metrics {
		total += metrics.VolatilityQueue
	}

	return total / float64(len(vc.metrics))
}

// CalculateAveragePerturbation 计算平均扰动
func (vc *VolatilityCalculator) CalculateAveragePerturbation() float64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	if len(vc.metrics) == 0 {
		return 0.0
	}

	total := 0.0
	for _, metrics := range vc.metrics {
		total += metrics.Perturbation
	}

	return total / float64(len(vc.metrics))
}

// Reset 重置所有波动性指标
func (vc *VolatilityCalculator) Reset() {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	vc.metrics = make(map[string]*VolatilityMetrics)
	vc.history = make(map[string][]*VolatilityMetrics)
}

// ValidateConfig 验证配置参数
func (vc *VolatilityCalculator) ValidateConfig() error {
	if vc.config.CongestionThreshold < 0 {
		return fmt.Errorf("congestion threshold must be non-negative, got: %f", vc.config.CongestionThreshold)
	}

	if vc.config.DecayFactor < 0 || vc.config.DecayFactor > 1 {
		return fmt.Errorf("decay factor must be in range [0, 1], got: %f", vc.config.DecayFactor)
	}

	if vc.config.SensitivityWeight < 0 {
		return fmt.Errorf("sensitivity weight must be non-negative, got: %f", vc.config.SensitivityWeight)
	}

	if vc.config.StabilityCapacity < 0 {
		return fmt.Errorf("stability capacity must be non-negative, got: %f", vc.config.StabilityCapacity)
	}

	return nil
}
