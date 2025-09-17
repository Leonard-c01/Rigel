package multiplex

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/xtaci/smux"
)

// MultiplexConfig 多路复用配置
type MultiplexConfig struct {
	// smux配置
	KeepAliveInterval time.Duration `json:"keep_alive_interval"` // 心跳间隔
	KeepAliveTimeout  time.Duration `json:"keep_alive_timeout"`  // 心跳超时
	MaxFrameSize      int           `json:"max_frame_size"`      // 最大帧大小
	MaxReceiveBuffer  int           `json:"max_receive_buffer"`  // 最大接收缓冲区
	MaxStreamBuffer   int           `json:"max_stream_buffer"`   // 最大流缓冲区

	// 会话管理配置
	MaxStreamsPerSession int           `json:"max_streams_per_session"` // 每个会话最大流数
	SessionTimeout       time.Duration `json:"session_timeout"`         // 会话超时
	StreamTimeout        time.Duration `json:"stream_timeout"`          // 流超时

	// 连接池配置
	MaxSessions        int           `json:"max_sessions"`         // 最大会话数
	MinSessions        int           `json:"min_sessions"`         // 最小会话数
	SessionIdleTimeout time.Duration `json:"session_idle_timeout"` // 会话空闲超时
}

// DefaultMultiplexConfig 默认多路复用配置
func DefaultMultiplexConfig() *MultiplexConfig {
	return &MultiplexConfig{
		KeepAliveInterval: 30 * time.Second,
		KeepAliveTimeout:  90 * time.Second,
		MaxFrameSize:      32768,   // 32KB
		MaxReceiveBuffer:  4194304, // 4MB
		MaxStreamBuffer:   65536,   // 64KB

		MaxStreamsPerSession: 256,
		SessionTimeout:       5 * time.Minute,
		StreamTimeout:        30 * time.Second,

		MaxSessions:        10,
		MinSessions:        2,
		SessionIdleTimeout: 2 * time.Minute,
	}
}

// SessionStatus 会话状态
type SessionStatus int

const (
	SessionStatusConnecting SessionStatus = iota
	SessionStatusActive
	SessionStatusIdle
	SessionStatusClosing
	SessionStatusClosed
)

func (s SessionStatus) String() string {
	switch s {
	case SessionStatusConnecting:
		return "connecting"
	case SessionStatusActive:
		return "active"
	case SessionStatusIdle:
		return "idle"
	case SessionStatusClosing:
		return "closing"
	case SessionStatusClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// MuxSession 多路复用会话
type MuxSession struct {
	ID          string
	Session     *smux.Session
	Target      string
	Status      SessionStatus
	CreatedAt   time.Time
	LastUsed    time.Time
	StreamCount int32
	BytesSent   int64
	BytesRecv   int64
	mu          sync.RWMutex
}

// GetStatus 获取会话状态
func (ms *MuxSession) GetStatus() SessionStatus {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.Status
}

// SetStatus 设置会话状态
func (ms *MuxSession) SetStatus(status SessionStatus) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.Status = status
	if status == SessionStatusActive {
		ms.LastUsed = time.Now()
	}
}

// UpdateStats 更新统计信息
func (ms *MuxSession) UpdateStats(sent, recv int64) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.BytesSent += sent
	ms.BytesRecv += recv
	ms.LastUsed = time.Now()
}

// IncrementStreamCount 增加流计数
func (ms *MuxSession) IncrementStreamCount() int32 {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.StreamCount++
	return ms.StreamCount
}

// DecrementStreamCount 减少流计数
func (ms *MuxSession) DecrementStreamCount() int32 {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.StreamCount > 0 {
		ms.StreamCount--
	}
	return ms.StreamCount
}

// GetStreamCount 获取流计数
func (ms *MuxSession) GetStreamCount() int32 {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.StreamCount
}

// IsHealthy 检查会话是否健康
func (ms *MuxSession) IsHealthy() bool {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if ms.Session == nil || ms.Session.IsClosed() {
		return false
	}

	if ms.Status == SessionStatusClosed || ms.Status == SessionStatusClosing {
		return false
	}

	return true
}

// MuxStream 多路复用流包装器
type MuxStream struct {
	ID        string
	Stream    *smux.Stream
	SessionID string
	CreatedAt time.Time
	LastUsed  time.Time
	BytesSent int64
	BytesRecv int64
	mu        sync.RWMutex
}

// UpdateStats 更新流统计信息
func (ms *MuxStream) UpdateStats(sent, recv int64) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.BytesSent += sent
	ms.BytesRecv += recv
	ms.LastUsed = time.Now()
}

// GetStats 获取流统计信息
func (ms *MuxStream) GetStats() (int64, int64) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.BytesSent, ms.BytesRecv
}

// SessionManager 会话管理器接口
type SessionManager interface {
	// CreateSession 创建新会话
	CreateSession(ctx context.Context, target string) (*MuxSession, error)

	// GetSession 获取可用会话
	GetSession(target string) (*MuxSession, error)

	// ReturnSession 归还会话
	ReturnSession(session *MuxSession)

	// CloseSession 关闭会话
	CloseSession(sessionID string) error

	// GetStats 获取会话管理器统计
	GetStats() *SessionManagerStats

	// Close 关闭会话管理器
	Close() error
}

// SessionManagerStats 会话管理器统计
type SessionManagerStats struct {
	TotalSessions  int   `json:"total_sessions"`
	ActiveSessions int   `json:"active_sessions"`
	IdleSessions   int   `json:"idle_sessions"`
	TotalStreams   int32 `json:"total_streams"`
	TotalBytesSent int64 `json:"total_bytes_sent"`
	TotalBytesRecv int64 `json:"total_bytes_recv"`
}

// StreamManager 流管理器接口
type StreamManager interface {
	// OpenStream 打开新流
	OpenStream(sessionID string) (*MuxStream, error)

	// CloseStream 关闭流
	CloseStream(streamID string) error

	// GetStream 获取流
	GetStream(streamID string) (*MuxStream, error)

	// GetStats 获取流管理器统计
	GetStats() *StreamManagerStats
}

// StreamManagerStats 流管理器统计
type StreamManagerStats struct {
	TotalStreams   int   `json:"total_streams"`
	ActiveStreams  int   `json:"active_streams"`
	TotalBytesSent int64 `json:"total_bytes_sent"`
	TotalBytesRecv int64 `json:"total_bytes_recv"`
}

// MultiplexConnection 多路复用连接接口
type MultiplexConnection interface {
	// Connect 建立多路复用连接
	Connect(ctx context.Context, target string) error

	// OpenStream 打开新的逻辑流
	OpenStream() (net.Conn, error)

	// Send 发送数据（自动选择或创建流）
	Send(data []byte) error

	// Receive 接收数据
	Receive() ([]byte, error)

	// Close 关闭多路复用连接
	Close() error

	// GetStats 获取统计信息
	GetStats() *MultiplexStats

	// Scale 动态调整会话数
	Scale(targetSessions int) error
}

// MultiplexStats 多路复用统计
type MultiplexStats struct {
	SessionCount   int                  `json:"session_count"`
	StreamCount    int32                `json:"stream_count"`
	TotalBytesSent int64                `json:"total_bytes_sent"`
	TotalBytesRecv int64                `json:"total_bytes_recv"`
	SessionStats   *SessionManagerStats `json:"session_stats"`
	StreamStats    *StreamManagerStats  `json:"stream_stats"`
}

// ConnectionMode 连接模式
type ConnectionMode int

const (
	ModeMultiplexOnly ConnectionMode = iota // 仅多路复用
	ModeParallelOnly                        // 仅并行连接
	ModeHybrid                              // 混合模式
	ModeAuto                                // 自动选择
)

func (m ConnectionMode) String() string {
	switch m {
	case ModeMultiplexOnly:
		return "multiplex_only"
	case ModeParallelOnly:
		return "parallel_only"
	case ModeHybrid:
		return "hybrid"
	case ModeAuto:
		return "auto"
	default:
		return "unknown"
	}
}
