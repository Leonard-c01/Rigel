package client

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rigel/pkg/log"
)

// TaskManager 任务管理器 - 负责将大任务拆分为标准化数据块
type TaskManager struct {
	// 配置
	chunkSize      int
	maxConcurrency int

	// 统计
	totalTasks  int64
	totalChunks int64
	totalBytes  int64
	activeTasks int64

	// 控制
	semaphore chan struct{} // 并发控制
	mu        sync.RWMutex
}

// TaskManagerStats 任务管理器统计
type TaskManagerStats struct {
	TotalTasks    int64   `json:"total_tasks"`
	TotalChunks   int64   `json:"total_chunks"`
	TotalBytes    int64   `json:"total_bytes"`
	ActiveTasks   int64   `json:"active_tasks"`
	AverageChunks float64 `json:"average_chunks_per_task"`
}

// NewTaskManager 创建任务管理器
func NewTaskManager(config *ClientConfig) (*TaskManager, error) {
	if config.ChunkSize <= 0 {
		return nil, fmt.Errorf("invalid chunk size: %d", config.ChunkSize)
	}

	if config.MaxConcurrency <= 0 {
		return nil, fmt.Errorf("invalid max concurrency: %d", config.MaxConcurrency)
	}

	tm := &TaskManager{
		chunkSize:      config.ChunkSize,
		maxConcurrency: config.MaxConcurrency,
		semaphore:      make(chan struct{}, config.MaxConcurrency),
	}

	log.Infof("Task manager created: chunk_size=%d, max_concurrency=%d",
		config.ChunkSize, config.MaxConcurrency)

	return tm, nil
}

// SplitIntoChunks 将数据拆分为标准化数据块
func (tm *TaskManager) SplitIntoChunks(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}

	// 获取并发控制令牌
	tm.semaphore <- struct{}{}
	defer func() { <-tm.semaphore }()

	atomic.AddInt64(&tm.activeTasks, 1)
	defer atomic.AddInt64(&tm.activeTasks, -1)

	atomic.AddInt64(&tm.totalTasks, 1)
	atomic.AddInt64(&tm.totalBytes, int64(len(data)))

	// 计算需要的块数
	totalSize := len(data)
	numChunks := (totalSize + tm.chunkSize - 1) / tm.chunkSize

	log.Debugf("Splitting %d bytes into %d chunks (chunk_size=%d)",
		totalSize, numChunks, tm.chunkSize)

	chunks := make([][]byte, 0, numChunks)

	for i := 0; i < numChunks; i++ {
		start := i * tm.chunkSize
		end := start + tm.chunkSize

		if end > totalSize {
			end = totalSize
		}

		// 创建数据块副本
		chunk := make([]byte, end-start)
		copy(chunk, data[start:end])

		chunks = append(chunks, chunk)

		log.Debugf("Created chunk %d: %d bytes (offset=%d-%d)",
			i, len(chunk), start, end-1)
	}

	atomic.AddInt64(&tm.totalChunks, int64(len(chunks)))

	log.Debugf("Data splitting completed: %d chunks created", len(chunks))
	return chunks, nil
}

// SplitFile 将文件拆分为数据块（流式处理）
func (tm *TaskManager) SplitFile(filePath string) (<-chan []byte, error) {
	// 这里可以实现文件的流式读取和分块
	// 为了演示，暂时返回一个简单的实现
	chunkChan := make(chan []byte, 10)

	go func() {
		defer close(chunkChan)

		// 模拟文件读取
		fileContent := []byte(fmt.Sprintf("File content from %s", filePath))
		chunks, err := tm.SplitIntoChunks(fileContent)
		if err != nil {
			log.Errorf("Failed to split file %s: %v", filePath, err)
			return
		}

		for _, chunk := range chunks {
			chunkChan <- chunk
		}
	}()

	return chunkChan, nil
}

// SplitStream 将流数据拆分为数据块
func (tm *TaskManager) SplitStream(dataStream <-chan []byte) <-chan []byte {
	chunkChan := make(chan []byte, 10)

	go func() {
		defer close(chunkChan)

		buffer := make([]byte, 0, tm.chunkSize*2) // 缓冲区

		for data := range dataStream {
			buffer = append(buffer, data...)

			// 当缓冲区足够大时，拆分为块
			for len(buffer) >= tm.chunkSize {
				chunk := make([]byte, tm.chunkSize)
				copy(chunk, buffer[:tm.chunkSize])

				chunkChan <- chunk
				buffer = buffer[tm.chunkSize:]

				atomic.AddInt64(&tm.totalChunks, 1)
			}
		}

		// 处理剩余数据
		if len(buffer) > 0 {
			chunk := make([]byte, len(buffer))
			copy(chunk, buffer)
			chunkChan <- chunk

			atomic.AddInt64(&tm.totalChunks, 1)
		}
	}()

	return chunkChan
}

// OptimizeChunkSize 根据网络条件优化块大小
func (tm *TaskManager) OptimizeChunkSize(networkCondition *NetworkCondition) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	oldSize := tm.chunkSize

	// 根据网络条件调整块大小
	if networkCondition.Bandwidth > 10*1024*1024 { // > 10Mbps
		tm.chunkSize = 128 * 1024 // 128KB
	} else if networkCondition.Bandwidth < 1*1024*1024 { // < 1Mbps
		tm.chunkSize = 32 * 1024 // 32KB
	} else {
		tm.chunkSize = 64 * 1024 // 64KB (default)
	}

	// 根据延迟调整
	if networkCondition.Latency > 200 { // > 200ms
		tm.chunkSize = tm.chunkSize / 2 // 减小块大小
	}

	if tm.chunkSize != oldSize {
		log.Infof("Chunk size optimized: %d -> %d (bandwidth=%.2f Mbps, latency=%d ms)",
			oldSize, tm.chunkSize,
			float64(networkCondition.Bandwidth)/1024/1024,
			networkCondition.Latency)
	}
}

// GetStats 获取统计信息
func (tm *TaskManager) GetStats() *TaskManagerStats {
	totalTasks := atomic.LoadInt64(&tm.totalTasks)
	totalChunks := atomic.LoadInt64(&tm.totalChunks)

	var averageChunks float64
	if totalTasks > 0 {
		averageChunks = float64(totalChunks) / float64(totalTasks)
	}

	return &TaskManagerStats{
		TotalTasks:    totalTasks,
		TotalChunks:   totalChunks,
		TotalBytes:    atomic.LoadInt64(&tm.totalBytes),
		ActiveTasks:   atomic.LoadInt64(&tm.activeTasks),
		AverageChunks: averageChunks,
	}
}

// Close 关闭任务管理器
func (tm *TaskManager) Close() error {
	log.Info("Task manager closing")

	// 等待所有活跃任务完成
	for atomic.LoadInt64(&tm.activeTasks) > 0 {
		time.Sleep(100 * time.Millisecond)
	}

	log.Info("Task manager closed")
	return nil
}

// NetworkCondition 网络条件
type NetworkCondition struct {
	Bandwidth int64   // bytes/sec
	Latency   int     // milliseconds
	LossRate  float64 // 0.0 - 1.0
}

// TaskInfo 任务信息
type TaskInfo struct {
	TaskID      string
	TotalSize   int
	ChunkCount  int
	CreatedAt   time.Time
	CompletedAt *time.Time
	Status      TaskStatus
}

// TaskStatus 任务状态
type TaskStatus int

const (
	TaskStatusPending TaskStatus = iota
	TaskStatusProcessing
	TaskStatusCompleted
	TaskStatusFailed
)

func (s TaskStatus) String() string {
	switch s {
	case TaskStatusPending:
		return "pending"
	case TaskStatusProcessing:
		return "processing"
	case TaskStatusCompleted:
		return "completed"
	case TaskStatusFailed:
		return "failed"
	default:
		return "unknown"
	}
}
