package parallel

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rigel/pkg/log"
)

// RoundRobinSplitter 轮询数据分发器
type RoundRobinSplitter struct {
	strategy   SplitStrategy
	counter    uint64
	sequenceID uint64
	mu         sync.RWMutex
}

// NewRoundRobinSplitter 创建轮询分发器
func NewRoundRobinSplitter() *RoundRobinSplitter {
	return &RoundRobinSplitter{
		strategy: SplitStrategyRoundRobin,
	}
}

// Split 分发数据到多个连接
func (s *RoundRobinSplitter) Split(data []byte, connections []*ManagedConnection) ([]*DataSegment, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}

	if len(connections) == 0 {
		return nil, fmt.Errorf("no available connections")
	}

	// 过滤健康的连接
	healthyConns := make([]*ManagedConnection, 0, len(connections))
	for _, conn := range connections {
		if conn.IsHealthy() && conn.GetStatus() != ConnectionStatusClosed {
			healthyConns = append(healthyConns, conn)
		}
	}

	if len(healthyConns) == 0 {
		return nil, fmt.Errorf("no healthy connections available")
	}

	// 计算每个连接的数据块大小
	chunkSize := len(data) / len(healthyConns)
	if chunkSize == 0 {
		chunkSize = 1
	}

	segments := make([]*DataSegment, 0, len(healthyConns))
	offset := 0

	for i, conn := range healthyConns {
		var chunk []byte
		isLast := (i == len(healthyConns)-1)

		if isLast {
			// 最后一个连接处理剩余所有数据
			chunk = data[offset:]
		} else {
			endOffset := offset + chunkSize
			if endOffset > len(data) {
				endOffset = len(data)
			}
			chunk = data[offset:endOffset]
		}

		if len(chunk) > 0 {
			segment := &DataSegment{
				SequenceID:   atomic.AddUint64(&s.sequenceID, 1),
				ConnectionID: conn.ID,
				Data:         chunk,
				Length:       len(chunk),
				Timestamp:    time.Now(),
				IsLast:       isLast,
			}
			segments = append(segments, segment)
		}

		offset += chunkSize
		if offset >= len(data) {
			break
		}
	}

	log.Debugf("Split %d bytes into %d segments using round-robin strategy",
		len(data), len(segments))

	return segments, nil
}

// GetStrategy 获取分发策略
func (s *RoundRobinSplitter) GetStrategy() SplitStrategy {
	return s.strategy
}

// SetStrategy 设置分发策略
func (s *RoundRobinSplitter) SetStrategy(strategy SplitStrategy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.strategy = strategy
}

// WeightedSplitter 权重数据分发器
type WeightedSplitter struct {
	strategy   SplitStrategy
	weights    map[string]float64
	sequenceID uint64
	mu         sync.RWMutex
}

// NewWeightedSplitter 创建权重分发器
func NewWeightedSplitter() *WeightedSplitter {
	return &WeightedSplitter{
		strategy: SplitStrategyWeighted,
		weights:  make(map[string]float64),
	}
}

// SetWeight 设置连接权重
func (s *WeightedSplitter) SetWeight(connectionID string, weight float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.weights[connectionID] = weight
}

// Split 按权重分发数据
func (s *WeightedSplitter) Split(data []byte, connections []*ManagedConnection) ([]*DataSegment, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}

	if len(connections) == 0 {
		return nil, fmt.Errorf("no available connections")
	}

	// 过滤健康的连接并计算总权重
	healthyConns := make([]*ManagedConnection, 0, len(connections))
	totalWeight := 0.0

	s.mu.RLock()
	for _, conn := range connections {
		if conn.IsHealthy() && conn.GetStatus() != ConnectionStatusClosed {
			weight := s.weights[conn.ID]
			if weight <= 0 {
				weight = 1.0 // 默认权重
			}
			totalWeight += weight
			healthyConns = append(healthyConns, conn)
		}
	}
	s.mu.RUnlock()

	if len(healthyConns) == 0 {
		return nil, fmt.Errorf("no healthy connections available")
	}

	segments := make([]*DataSegment, 0, len(healthyConns))
	offset := 0
	dataLen := len(data)

	for i, conn := range healthyConns {
		weight := s.weights[conn.ID]
		if weight <= 0 {
			weight = 1.0
		}

		// 计算这个连接应该处理的数据比例
		ratio := weight / totalWeight
		chunkSize := int(float64(dataLen) * ratio)

		// 确保最后一个连接处理剩余所有数据
		isLast := (i == len(healthyConns)-1)
		if isLast {
			chunkSize = dataLen - offset
		}

		if chunkSize > 0 && offset < dataLen {
			endOffset := offset + chunkSize
			if endOffset > dataLen {
				endOffset = dataLen
			}

			chunk := data[offset:endOffset]
			segment := &DataSegment{
				SequenceID:   atomic.AddUint64(&s.sequenceID, 1),
				ConnectionID: conn.ID,
				Data:         chunk,
				Length:       len(chunk),
				Timestamp:    time.Now(),
				IsLast:       isLast,
			}
			segments = append(segments, segment)
			offset = endOffset
		}

		if offset >= dataLen {
			break
		}
	}

	log.Debugf("Split %d bytes into %d segments using weighted strategy",
		len(data), len(segments))

	return segments, nil
}

// GetStrategy 获取分发策略
func (s *WeightedSplitter) GetStrategy() SplitStrategy {
	return s.strategy
}

// SetStrategy 设置分发策略
func (s *WeightedSplitter) SetStrategy(strategy SplitStrategy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.strategy = strategy
}

// LeastConnSplitter 最少连接数分发器
type LeastConnSplitter struct {
	strategy   SplitStrategy
	sequenceID uint64
	mu         sync.RWMutex
}

// NewLeastConnSplitter 创建最少连接分发器
func NewLeastConnSplitter() *LeastConnSplitter {
	return &LeastConnSplitter{
		strategy: SplitStrategyLeastConn,
	}
}

// Split 按最少连接数分发数据
func (s *LeastConnSplitter) Split(data []byte, connections []*ManagedConnection) ([]*DataSegment, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}

	if len(connections) == 0 {
		return nil, fmt.Errorf("no available connections")
	}

	// 过滤健康的连接并按负载排序
	healthyConns := make([]*ManagedConnection, 0, len(connections))
	for _, conn := range connections {
		if conn.IsHealthy() && conn.GetStatus() != ConnectionStatusClosed {
			healthyConns = append(healthyConns, conn)
		}
	}

	if len(healthyConns) == 0 {
		return nil, fmt.Errorf("no healthy connections available")
	}

	// 简单实现：选择错误最少的连接
	bestConn := healthyConns[0]
	for _, conn := range healthyConns[1:] {
		if conn.ErrorCount < bestConn.ErrorCount {
			bestConn = conn
		}
	}

	// 将所有数据分配给最佳连接
	segment := &DataSegment{
		SequenceID:   atomic.AddUint64(&s.sequenceID, 1),
		ConnectionID: bestConn.ID,
		Data:         data,
		Length:       len(data),
		Timestamp:    time.Now(),
		IsLast:       true,
	}

	log.Debugf("Split %d bytes to connection %s using least-conn strategy",
		len(data), bestConn.ID)

	return []*DataSegment{segment}, nil
}

// GetStrategy 获取分发策略
func (s *LeastConnSplitter) GetStrategy() SplitStrategy {
	return s.strategy
}

// SetStrategy 设置分发策略
func (s *LeastConnSplitter) SetStrategy(strategy SplitStrategy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.strategy = strategy
}

// RandomSplitter 随机分发器
type RandomSplitter struct {
	strategy   SplitStrategy
	sequenceID uint64
	rand       *rand.Rand
	mu         sync.RWMutex
}

// NewRandomSplitter 创建随机分发器
func NewRandomSplitter() *RandomSplitter {
	return &RandomSplitter{
		strategy: SplitStrategyRandom,
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Split 随机分发数据
func (s *RandomSplitter) Split(data []byte, connections []*ManagedConnection) ([]*DataSegment, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}

	if len(connections) == 0 {
		return nil, fmt.Errorf("no available connections")
	}

	// 过滤健康的连接
	healthyConns := make([]*ManagedConnection, 0, len(connections))
	for _, conn := range connections {
		if conn.IsHealthy() && conn.GetStatus() != ConnectionStatusClosed {
			healthyConns = append(healthyConns, conn)
		}
	}

	if len(healthyConns) == 0 {
		return nil, fmt.Errorf("no healthy connections available")
	}

	// 随机选择一个连接
	s.mu.Lock()
	selectedConn := healthyConns[s.rand.Intn(len(healthyConns))]
	s.mu.Unlock()

	segment := &DataSegment{
		SequenceID:   atomic.AddUint64(&s.sequenceID, 1),
		ConnectionID: selectedConn.ID,
		Data:         data,
		Length:       len(data),
		Timestamp:    time.Now(),
		IsLast:       true,
	}

	log.Debugf("Split %d bytes to connection %s using random strategy",
		len(data), selectedConn.ID)

	return []*DataSegment{segment}, nil
}

// GetStrategy 获取分发策略
func (s *RandomSplitter) GetStrategy() SplitStrategy {
	return s.strategy
}

// SetStrategy 设置分发策略
func (s *RandomSplitter) SetStrategy(strategy SplitStrategy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.strategy = strategy
}

// NewDataSplitter 根据策略创建数据分发器
func NewDataSplitter(strategy SplitStrategy) DataSplitter {
	switch strategy {
	case SplitStrategyRoundRobin:
		return NewRoundRobinSplitter()
	case SplitStrategyWeighted:
		return NewWeightedSplitter()
	case SplitStrategyLeastConn:
		return NewLeastConnSplitter()
	case SplitStrategyRandom:
		return NewRandomSplitter()
	default:
		return NewRoundRobinSplitter()
	}
}
