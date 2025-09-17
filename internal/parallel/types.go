package parallel

import (
	"context"
	"net"
	"sync"
	"time"
)

// ConnectionConfig TCP并行连接配置
type ConnectionConfig struct {
	MaxConnections      int           `json:"max_connections"`       // 最大连接数
	MinConnections      int           `json:"min_connections"`       // 最小连接数
	InitialConnections  int           `json:"initial_connections"`   // 初始连接数
	ConnectTimeout      time.Duration `json:"connect_timeout"`       // 连接超时
	IdleTimeout         time.Duration `json:"idle_timeout"`          // 空闲超时
	HealthCheckInterval time.Duration `json:"health_check_interval"` // 健康检查间隔
	RetryInterval       time.Duration `json:"retry_interval"`        // 重试间隔
	MaxRetries          int           `json:"max_retries"`           // 最大重试次数
	BufferSize          int           `json:"buffer_size"`           // 缓冲区大小
}

// DefaultConnectionConfig 默认配置
func DefaultConnectionConfig() *ConnectionConfig {
	return &ConnectionConfig{
		MaxConnections:      10,
		MinConnections:      2,
		InitialConnections:  3,
		ConnectTimeout:      30 * time.Second,
		IdleTimeout:         5 * time.Minute,
		HealthCheckInterval: 30 * time.Second,
		RetryInterval:       5 * time.Second,
		MaxRetries:          3,
		BufferSize:          64 * 1024, // 64KB
	}
}

// ConnectionStatus 连接状态
type ConnectionStatus int

const (
	ConnectionStatusIdle ConnectionStatus = iota
	ConnectionStatusActive
	ConnectionStatusConnecting
	ConnectionStatusError
	ConnectionStatusClosed
)

func (s ConnectionStatus) String() string {
	switch s {
	case ConnectionStatusIdle:
		return "idle"
	case ConnectionStatusActive:
		return "active"
	case ConnectionStatusConnecting:
		return "connecting"
	case ConnectionStatusError:
		return "error"
	case ConnectionStatusClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// ManagedConnection 管理的连接
type ManagedConnection struct {
	ID         string
	Conn       net.Conn
	Status     ConnectionStatus
	CreatedAt  time.Time
	LastUsed   time.Time
	BytesSent  int64
	BytesRecv  int64
	ErrorCount int
	mu         sync.RWMutex
}

// GetStatus 获取连接状态
func (mc *ManagedConnection) GetStatus() ConnectionStatus {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.Status
}

// SetStatus 设置连接状态
func (mc *ManagedConnection) SetStatus(status ConnectionStatus) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.Status = status
	if status == ConnectionStatusActive {
		mc.LastUsed = time.Now()
	}
}

// UpdateStats 更新统计信息
func (mc *ManagedConnection) UpdateStats(sent, recv int64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.BytesSent += sent
	mc.BytesRecv += recv
	mc.LastUsed = time.Now()
}

// IncrementError 增加错误计数
func (mc *ManagedConnection) IncrementError() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.ErrorCount++
}

// IsHealthy 检查连接是否健康
func (mc *ManagedConnection) IsHealthy() bool {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	// 检查连接状态
	if mc.Status == ConnectionStatusError || mc.Status == ConnectionStatusClosed {
		return false
	}

	// 检查错误率
	if mc.ErrorCount > 5 {
		return false
	}

	return true
}

// DataSegment 数据段
type DataSegment struct {
	SequenceID   uint64    // 序列号
	ConnectionID string    // 连接ID
	Data         []byte    // 数据内容
	Length       int       // 数据长度
	Timestamp    time.Time // 时间戳
	IsLast       bool      // 是否最后一段
}

// ConnectionPool 连接池接口
type ConnectionPool interface {
	// Start 启动连接池
	Start(ctx context.Context) error

	// Stop 停止连接池
	Stop() error

	// GetConnection 获取可用连接
	GetConnection() (*ManagedConnection, error)

	// ReturnConnection 归还连接
	ReturnConnection(conn *ManagedConnection)

	// GetStats 获取连接池统计
	GetStats() *PoolStats

	// Scale 动态调整连接数
	Scale(targetSize int) error
}

// PoolStats 连接池统计
type PoolStats struct {
	TotalConnections  int   `json:"total_connections"`
	ActiveConnections int   `json:"active_connections"`
	IdleConnections   int   `json:"idle_connections"`
	ErrorConnections  int   `json:"error_connections"`
	TotalBytesSent    int64 `json:"total_bytes_sent"`
	TotalBytesRecv    int64 `json:"total_bytes_recv"`
	ConnectionErrors  int64 `json:"connection_errors"`
}

// DataSplitter 数据分发器接口
type DataSplitter interface {
	// Split 分发数据到多个连接
	Split(data []byte, connections []*ManagedConnection) ([]*DataSegment, error)

	// GetStrategy 获取分发策略
	GetStrategy() SplitStrategy

	// SetStrategy 设置分发策略
	SetStrategy(strategy SplitStrategy)
}

// SplitStrategy 分发策略
type SplitStrategy int

const (
	SplitStrategyRoundRobin SplitStrategy = iota // 轮询
	SplitStrategyWeighted                        // 权重
	SplitStrategyLeastConn                       // 最少连接
	SplitStrategyRandom                          // 随机
)

func (s SplitStrategy) String() string {
	switch s {
	case SplitStrategyRoundRobin:
		return "round_robin"
	case SplitStrategyWeighted:
		return "weighted"
	case SplitStrategyLeastConn:
		return "least_conn"
	case SplitStrategyRandom:
		return "random"
	default:
		return "unknown"
	}
}

// DataReassembler 数据重组器接口
type DataReassembler interface {
	// AddSegment 添加数据段
	AddSegment(segment *DataSegment) error

	// GetCompleteData 获取完整数据
	GetCompleteData() ([]byte, bool)

	// Reset 重置重组器
	Reset()

	// GetStats 获取重组统计
	GetStats() *ReassemblerStats
}

// ReassemblerStats 重组器统计
type ReassemblerStats struct {
	TotalSegments     int64 `json:"total_segments"`
	CompletedMessages int64 `json:"completed_messages"`
	PendingSegments   int   `json:"pending_segments"`
	OutOfOrderCount   int64 `json:"out_of_order_count"`
	DuplicateCount    int64 `json:"duplicate_count"`
}

// ParallelConnection TCP并行连接管理器接口
type ParallelConnection interface {
	// Connect 建立并行连接
	Connect(ctx context.Context, target string) error

	// Send 发送数据
	Send(data []byte) error

	// Receive 接收数据
	Receive() ([]byte, error)

	// Close 关闭所有连接
	Close() error

	// GetStats 获取统计信息
	GetStats() *ParallelStats

	// Scale 动态调整连接数
	Scale(targetSize int) error
}

// ParallelStats 并行连接统计
type ParallelStats struct {
	ConnectionCount   int        `json:"connection_count"`
	TotalBytesSent    int64      `json:"total_bytes_sent"`
	TotalBytesRecv    int64      `json:"total_bytes_recv"`
	AverageThroughput float64    `json:"average_throughput"` // Mbps
	ConnectionErrors  int64      `json:"connection_errors"`
	Pool              *PoolStats `json:"pool_stats"`
}
