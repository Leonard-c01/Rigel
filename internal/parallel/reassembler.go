package parallel

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/rigel/pkg/log"
)

// SequentialReassembler 顺序数据重组器
type SequentialReassembler struct {
	segments     map[uint64]*DataSegment
	expectedSeq  uint64
	buffer       *bytes.Buffer
	mu           sync.RWMutex
	stats        *ReassemblerStats
	timeout      time.Duration
	lastActivity time.Time
}

// NewSequentialReassembler 创建顺序重组器
func NewSequentialReassembler() *SequentialReassembler {
	return &SequentialReassembler{
		segments:     make(map[uint64]*DataSegment),
		expectedSeq:  1,
		buffer:       bytes.NewBuffer(nil),
		timeout:      30 * time.Second,
		lastActivity: time.Now(),
		stats: &ReassemblerStats{
			TotalSegments:     0,
			CompletedMessages: 0,
			PendingSegments:   0,
			OutOfOrderCount:   0,
			DuplicateCount:    0,
		},
	}
}

// AddSegment 添加数据段
func (r *SequentialReassembler) AddSegment(segment *DataSegment) error {
	if segment == nil {
		return fmt.Errorf("segment is nil")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.lastActivity = time.Now()
	r.stats.TotalSegments++

	// 检查是否是重复段
	if _, exists := r.segments[segment.SequenceID]; exists {
		r.stats.DuplicateCount++
		log.Debugf("Duplicate segment received: seq=%d", segment.SequenceID)
		return nil
	}

	// 存储段
	r.segments[segment.SequenceID] = segment
	r.stats.PendingSegments = len(r.segments)

	log.Debugf("Added segment: seq=%d, conn=%s, len=%d, isLast=%v",
		segment.SequenceID, segment.ConnectionID, segment.Length, segment.IsLast)

	// 尝试重组连续的段
	r.tryReassemble()

	return nil
}

// GetCompleteData 获取完整数据
func (r *SequentialReassembler) GetCompleteData() ([]byte, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.buffer.Len() == 0 {
		return nil, false
	}

	data := make([]byte, r.buffer.Len())
	copy(data, r.buffer.Bytes())
	return data, true
}

// Reset 重置重组器
func (r *SequentialReassembler) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.segments = make(map[uint64]*DataSegment)
	r.expectedSeq = 1
	r.buffer.Reset()
	r.lastActivity = time.Now()
	r.stats.PendingSegments = 0

	log.Debug("Reassembler reset")
}

// GetStats 获取重组统计
func (r *SequentialReassembler) GetStats() *ReassemblerStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats := *r.stats
	stats.PendingSegments = len(r.segments)
	return &stats
}

// tryReassemble 尝试重组连续的段
func (r *SequentialReassembler) tryReassemble() {
	for {
		segment, exists := r.segments[r.expectedSeq]
		if !exists {
			// 没有期望的下一个段，检查是否有乱序段
			if len(r.segments) > 0 {
				r.handleOutOfOrder()
			}
			break
		}

		// 将段数据写入缓冲区
		r.buffer.Write(segment.Data)
		delete(r.segments, r.expectedSeq)
		r.expectedSeq++

		log.Debugf("Reassembled segment: seq=%d, buffer_size=%d",
			segment.SequenceID, r.buffer.Len())

		// 如果这是最后一个段，标记消息完成
		if segment.IsLast {
			r.stats.CompletedMessages++
			log.Debugf("Message completed: total_size=%d", r.buffer.Len())
			break
		}
	}

	r.stats.PendingSegments = len(r.segments)
}

// handleOutOfOrder 处理乱序段
func (r *SequentialReassembler) handleOutOfOrder() {
	// 检查是否有可以处理的乱序段
	sequences := make([]uint64, 0, len(r.segments))
	for seq := range r.segments {
		sequences = append(sequences, seq)
	}
	sort.Slice(sequences, func(i, j int) bool {
		return sequences[i] < sequences[j]
	})

	// 如果最小的序列号比期望的大，说明有段丢失
	if len(sequences) > 0 && sequences[0] > r.expectedSeq {
		r.stats.OutOfOrderCount++
		log.Warnf("Out of order segments detected: expected=%d, got=%d",
			r.expectedSeq, sequences[0])

		// 简单策略：跳过丢失的段，继续处理
		if time.Since(r.lastActivity) > r.timeout {
			log.Warnf("Timeout waiting for segment %d, skipping to %d",
				r.expectedSeq, sequences[0])
			r.expectedSeq = sequences[0]
		}
	}
}

// IsTimeout 检查是否超时
func (r *SequentialReassembler) IsTimeout() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return time.Since(r.lastActivity) > r.timeout
}

// BufferedReassembler 缓冲数据重组器（支持乱序处理）
type BufferedReassembler struct {
	segments        map[uint64]*DataSegment
	orderedSegments []uint64
	buffer          *bytes.Buffer
	mu              sync.RWMutex
	stats           *ReassemblerStats
	maxBufferSize   int
	timeout         time.Duration
	lastActivity    time.Time
	completed       bool
}

// NewBufferedReassembler 创建缓冲重组器
func NewBufferedReassembler(maxBufferSize int) *BufferedReassembler {
	return &BufferedReassembler{
		segments:      make(map[uint64]*DataSegment),
		buffer:        bytes.NewBuffer(nil),
		maxBufferSize: maxBufferSize,
		timeout:       30 * time.Second,
		lastActivity:  time.Now(),
		stats: &ReassemblerStats{
			TotalSegments:     0,
			CompletedMessages: 0,
			PendingSegments:   0,
			OutOfOrderCount:   0,
			DuplicateCount:    0,
		},
	}
}

// AddSegment 添加数据段
func (r *BufferedReassembler) AddSegment(segment *DataSegment) error {
	if segment == nil {
		return fmt.Errorf("segment is nil")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.completed {
		return fmt.Errorf("reassembler already completed")
	}

	r.lastActivity = time.Now()
	r.stats.TotalSegments++

	// 检查是否是重复段
	if _, exists := r.segments[segment.SequenceID]; exists {
		r.stats.DuplicateCount++
		return nil
	}

	// 检查缓冲区大小
	if r.buffer.Len()+segment.Length > r.maxBufferSize {
		return fmt.Errorf("buffer overflow: current=%d, adding=%d, max=%d",
			r.buffer.Len(), segment.Length, r.maxBufferSize)
	}

	// 存储段
	r.segments[segment.SequenceID] = segment
	r.orderedSegments = append(r.orderedSegments, segment.SequenceID)
	r.stats.PendingSegments = len(r.segments)

	log.Debugf("Added segment to buffer: seq=%d, conn=%s, len=%d",
		segment.SequenceID, segment.ConnectionID, segment.Length)

	// 检查是否可以完成重组
	if segment.IsLast {
		r.performReassembly()
	}

	return nil
}

// GetCompleteData 获取完整数据
func (r *BufferedReassembler) GetCompleteData() ([]byte, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.completed || r.buffer.Len() == 0 {
		return nil, false
	}

	data := make([]byte, r.buffer.Len())
	copy(data, r.buffer.Bytes())
	return data, true
}

// Reset 重置重组器
func (r *BufferedReassembler) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.segments = make(map[uint64]*DataSegment)
	r.orderedSegments = nil
	r.buffer.Reset()
	r.completed = false
	r.lastActivity = time.Now()
	r.stats.PendingSegments = 0
}

// GetStats 获取重组统计
func (r *BufferedReassembler) GetStats() *ReassemblerStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats := *r.stats
	stats.PendingSegments = len(r.segments)
	return &stats
}

// performReassembly 执行重组
func (r *BufferedReassembler) performReassembly() {
	// 按序列号排序
	sort.Slice(r.orderedSegments, func(i, j int) bool {
		return r.orderedSegments[i] < r.orderedSegments[j]
	})

	// 检查是否有缺失的段
	expectedSeq := uint64(1)
	for _, seq := range r.orderedSegments {
		if seq != expectedSeq {
			r.stats.OutOfOrderCount++
			log.Warnf("Missing segment detected: expected=%d, got=%d", expectedSeq, seq)
		}
		expectedSeq = seq + 1
	}

	// 按顺序重组数据
	r.buffer.Reset()
	for _, seq := range r.orderedSegments {
		if segment, exists := r.segments[seq]; exists {
			r.buffer.Write(segment.Data)
		}
	}

	r.completed = true
	r.stats.CompletedMessages++

	log.Infof("Reassembly completed: segments=%d, total_size=%d",
		len(r.orderedSegments), r.buffer.Len())
}

// IsCompleted 检查是否完成重组
func (r *BufferedReassembler) IsCompleted() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.completed
}

// IsTimeout 检查是否超时
func (r *BufferedReassembler) IsTimeout() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return time.Since(r.lastActivity) > r.timeout
}

// NewDataReassembler 创建数据重组器
func NewDataReassembler(buffered bool, maxBufferSize int) DataReassembler {
	if buffered {
		if maxBufferSize <= 0 {
			maxBufferSize = 1024 * 1024 // 1MB default
		}
		return NewBufferedReassembler(maxBufferSize)
	}
	return NewSequentialReassembler()
}
