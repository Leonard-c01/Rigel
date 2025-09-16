package hybrid

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rigel/pkg/log"

	"github.com/xtaci/smux"
)

// UnifiedConnectionConfig 统一连接配置
type UnifiedConnectionConfig struct {
	// 并行连接配置
	MaxConnections int    `json:"max_connections"` // 最大并行TCP连接数
	SplitStrategy  string `json:"split_strategy"`  // 数据分发策略

	// 多路复用配置
	MaxStreamsPerConnection int           `json:"max_streams_per_connection"` // 每个TCP连接的最大流数
	StreamBufferSize        int           `json:"stream_buffer_size"`         // 流缓冲区大小
	KeepAliveInterval       time.Duration `json:"keep_alive_interval"`        // 心跳间隔
	KeepAliveTimeout        time.Duration `json:"keep_alive_timeout"`         // 心跳超时

	// 连接管理配置
	ConnectionBufferSize int           `json:"connection_buffer_size"` // 连接缓冲区大小
	ConnectionTimeout    time.Duration `json:"connection_timeout"`     // 连接超时
	RetryInterval        time.Duration `json:"retry_interval"`         // 重试间隔
	MaxRetries           int           `json:"max_retries"`            // 最大重试次数
}

// DefaultUnifiedConnectionConfig 默认统一连接配置
func DefaultUnifiedConnectionConfig() *UnifiedConnectionConfig {
	return &UnifiedConnectionConfig{
		MaxConnections:          3,
		SplitStrategy:           "round_robin",
		MaxStreamsPerConnection: 10,
		StreamBufferSize:        32 * 1024,
		KeepAliveInterval:       30 * time.Second,
		KeepAliveTimeout:        90 * time.Second,
		ConnectionBufferSize:    64 * 1024,
		ConnectionTimeout:       10 * time.Second,
		RetryInterval:           2 * time.Second,
		MaxRetries:              3,
	}
}

// ConnectionWithMultiplex 带多路复用的连接
type ConnectionWithMultiplex struct {
	ID          string
	TCPConn     net.Conn
	SmuxSession *smux.Session
	StreamPool  chan *smux.Stream
	CreatedAt   time.Time
	LastUsed    time.Time
	StreamCount int32
	BytesSent   int64
	BytesRecv   int64
	ErrorCount  int32
	Status      ConnectionStatus
	mu          sync.RWMutex
}

// ConnectionStatus 连接状态
type ConnectionStatus int

const (
	ConnectionStatusConnecting ConnectionStatus = iota
	ConnectionStatusActive
	ConnectionStatusIdle
	ConnectionStatusError
	ConnectionStatusClosed
)

func (s ConnectionStatus) String() string {
	switch s {
	case ConnectionStatusConnecting:
		return "connecting"
	case ConnectionStatusActive:
		return "active"
	case ConnectionStatusIdle:
		return "idle"
	case ConnectionStatusError:
		return "error"
	case ConnectionStatusClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// GetStatus 获取连接状态
func (c *ConnectionWithMultiplex) GetStatus() ConnectionStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Status
}

// SetStatus 设置连接状态
func (c *ConnectionWithMultiplex) SetStatus(status ConnectionStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Status = status
	if status == ConnectionStatusActive {
		c.LastUsed = time.Now()
	}
}

// IsHealthy 检查连接是否健康
func (c *ConnectionWithMultiplex) IsHealthy() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.TCPConn == nil || c.SmuxSession == nil {
		return false
	}

	if c.SmuxSession.IsClosed() {
		return false
	}

	if c.Status == ConnectionStatusError || c.Status == ConnectionStatusClosed {
		return false
	}

	return true
}

// GetStream 获取流
func (c *ConnectionWithMultiplex) GetStream() (*smux.Stream, error) {
	if !c.IsHealthy() {
		return nil, fmt.Errorf("connection %s is not healthy", c.ID)
	}

	// 尝试从池中获取
	select {
	case stream := <-c.StreamPool:
		if stream != nil {
			// 简化检查：假设从池中获取的流都是可用的
			atomic.AddInt32(&c.StreamCount, 1)
			return stream, nil
		}
	default:
		// 池为空，创建新流
	}

	// 创建新流
	stream, err := c.SmuxSession.OpenStream()
	if err != nil {
		atomic.AddInt32(&c.ErrorCount, 1)
		c.SetStatus(ConnectionStatusError)
		return nil, fmt.Errorf("failed to open stream: %v", err)
	}

	atomic.AddInt32(&c.StreamCount, 1)
	c.SetStatus(ConnectionStatusActive)

	return stream, nil
}

// ReturnStream 归还流
func (c *ConnectionWithMultiplex) ReturnStream(stream *smux.Stream) {
	if stream == nil {
		return
	}

	atomic.AddInt32(&c.StreamCount, -1)

	// 简化实现：直接尝试归还流

	// 尝试归还到池中
	select {
	case c.StreamPool <- stream:
		// 成功归还
	default:
		// 池满，关闭流
		stream.Close()
	}
}

// UpdateStats 更新统计信息
func (c *ConnectionWithMultiplex) UpdateStats(sent, recv int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.BytesSent += sent
	c.BytesRecv += recv
	c.LastUsed = time.Now()
}

// Close 关闭连接
func (c *ConnectionWithMultiplex) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Status = ConnectionStatusClosed

	// 关闭流池
	close(c.StreamPool)
	for stream := range c.StreamPool {
		if stream != nil {
			stream.Close()
		}
	}

	// 关闭smux会话
	if c.SmuxSession != nil {
		c.SmuxSession.Close()
	}

	// 关闭TCP连接
	if c.TCPConn != nil {
		c.TCPConn.Close()
	}

	return nil
}

// UnifiedConnectionManager 统一连接管理器
type UnifiedConnectionManager struct {
	target      string
	config      *UnifiedConnectionConfig
	connections []*ConnectionWithMultiplex

	// 负载均衡
	roundRobinIndex int32

	// 统计信息
	totalBytesSent int64
	totalBytesRecv int64
	totalErrors    int64

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.RWMutex
	wg     sync.WaitGroup
}

// NewUnifiedConnectionManager 创建统一连接管理器
func NewUnifiedConnectionManager(target string, config *UnifiedConnectionConfig) *UnifiedConnectionManager {
	if config == nil {
		config = DefaultUnifiedConnectionConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &UnifiedConnectionManager{
		target:      target,
		config:      config,
		connections: make([]*ConnectionWithMultiplex, 0, config.MaxConnections),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Connect 建立统一连接
func (m *UnifiedConnectionManager) Connect(ctx context.Context, target string) error {
	log.Infof("Establishing unified connections (TCP+smux) to %s", target)

	m.target = target

	// 建立并行TCP连接，每个都配置smux多路复用
	for i := 0; i < m.config.MaxConnections; i++ {
		conn, err := m.createConnectionWithMultiplex(i)
		if err != nil {
			log.Errorf("Failed to create connection %d: %v", i, err)
			continue
		}

		m.mu.Lock()
		m.connections = append(m.connections, conn)
		m.mu.Unlock()

		log.Infof("Created connection %d: TCP+smux to %s", i+1, target)
	}

	if len(m.connections) == 0 {
		return fmt.Errorf("failed to create any connections")
	}

	// 启动维护协程
	m.wg.Add(1)
	go m.maintenanceLoop()

	log.Infof("Unified connection established: %d TCP+smux connections to %s",
		len(m.connections), target)

	return nil
}

// createConnectionWithMultiplex 创建带多路复用的连接
func (m *UnifiedConnectionManager) createConnectionWithMultiplex(index int) (*ConnectionWithMultiplex, error) {
	// 建立TCP连接
	tcpConn, err := net.DialTimeout("tcp", m.target, m.config.ConnectionTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to dial TCP: %v", err)
	}

	// 创建smux配置
	smuxConfig := smux.DefaultConfig()
	smuxConfig.KeepAliveInterval = m.config.KeepAliveInterval
	smuxConfig.KeepAliveTimeout = m.config.KeepAliveTimeout
	smuxConfig.MaxStreamBuffer = m.config.StreamBufferSize

	// 创建smux会话
	session, err := smux.Client(tcpConn, smuxConfig)
	if err != nil {
		tcpConn.Close()
		return nil, fmt.Errorf("failed to create smux session: %v", err)
	}

	// 创建连接对象
	conn := &ConnectionWithMultiplex{
		ID:          fmt.Sprintf("conn_%d_%d", index, time.Now().UnixNano()),
		TCPConn:     tcpConn,
		SmuxSession: session,
		StreamPool:  make(chan *smux.Stream, m.config.MaxStreamsPerConnection),
		CreatedAt:   time.Now(),
		LastUsed:    time.Now(),
		Status:      ConnectionStatusActive,
	}

	return conn, nil
}

// Send 发送数据
func (m *UnifiedConnectionManager) Send(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty data")
	}

	// 根据分发策略选择连接和流
	switch m.config.SplitStrategy {
	case "round_robin":
		return m.sendRoundRobin(data)
	case "least_streams":
		return m.sendLeastStreams(data)
	default:
		return m.sendRoundRobin(data)
	}
}

// sendRoundRobin 轮询发送
func (m *UnifiedConnectionManager) sendRoundRobin(data []byte) error {
	m.mu.RLock()
	connections := make([]*ConnectionWithMultiplex, len(m.connections))
	copy(connections, m.connections)
	m.mu.RUnlock()

	if len(connections) == 0 {
		return fmt.Errorf("no available connections")
	}

	// 选择连接
	index := atomic.AddInt32(&m.roundRobinIndex, 1) % int32(len(connections))
	conn := connections[index]

	// 获取流
	stream, err := conn.GetStream()
	if err != nil {
		return fmt.Errorf("failed to get stream: %v", err)
	}
	defer conn.ReturnStream(stream)

	// 发送数据
	_, err = stream.Write(data)
	if err != nil {
		atomic.AddInt32(&conn.ErrorCount, 1)
		return fmt.Errorf("failed to write data: %v", err)
	}

	// 更新统计
	conn.UpdateStats(int64(len(data)), 0)
	atomic.AddInt64(&m.totalBytesSent, int64(len(data)))

	return nil
}

// sendLeastStreams 最少流发送
func (m *UnifiedConnectionManager) sendLeastStreams(data []byte) error {
	m.mu.RLock()
	connections := make([]*ConnectionWithMultiplex, len(m.connections))
	copy(connections, m.connections)
	m.mu.RUnlock()

	if len(connections) == 0 {
		return fmt.Errorf("no available connections")
	}

	// 选择流数最少的连接
	var bestConn *ConnectionWithMultiplex
	minStreams := int32(1000000)

	for _, conn := range connections {
		if conn.IsHealthy() {
			streamCount := atomic.LoadInt32(&conn.StreamCount)
			if streamCount < minStreams {
				minStreams = streamCount
				bestConn = conn
			}
		}
	}

	if bestConn == nil {
		return fmt.Errorf("no healthy connections")
	}

	// 获取流并发送
	stream, err := bestConn.GetStream()
	if err != nil {
		return fmt.Errorf("failed to get stream: %v", err)
	}
	defer bestConn.ReturnStream(stream)

	_, err = stream.Write(data)
	if err != nil {
		atomic.AddInt32(&bestConn.ErrorCount, 1)
		return fmt.Errorf("failed to write data: %v", err)
	}

	bestConn.UpdateStats(int64(len(data)), 0)
	atomic.AddInt64(&m.totalBytesSent, int64(len(data)))

	return nil
}

// Receive 接收数据
func (m *UnifiedConnectionManager) Receive() ([]byte, error) {
	// 从所有连接的流中接收数据
	m.mu.RLock()
	connections := make([]*ConnectionWithMultiplex, len(m.connections))
	copy(connections, m.connections)
	m.mu.RUnlock()

	if len(connections) == 0 {
		return nil, fmt.Errorf("no available connections")
	}

	// 轮询方式从连接中接收数据
	for _, conn := range connections {
		if !conn.IsHealthy() {
			continue
		}

		stream, err := conn.GetStream()
		if err != nil {
			continue
		}

		// 设置读取超时
		stream.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

		buffer := make([]byte, m.config.StreamBufferSize)
		n, err := stream.Read(buffer)

		stream.SetReadDeadline(time.Time{})
		conn.ReturnStream(stream)

		if err != nil {
			continue
		}

		if n > 0 {
			data := buffer[:n]
			conn.UpdateStats(0, int64(n))
			atomic.AddInt64(&m.totalBytesRecv, int64(n))
			return data, nil
		}
	}

	return nil, fmt.Errorf("no data received")
}

// Close 关闭管理器
func (m *UnifiedConnectionManager) Close() error {
	log.Info("Closing unified connection manager")

	m.cancel()
	m.wg.Wait()

	m.mu.Lock()
	defer m.mu.Unlock()

	// 关闭所有连接
	for _, conn := range m.connections {
		if err := conn.Close(); err != nil {
			log.Errorf("Error closing connection %s: %v", conn.ID, err)
		}
	}

	m.connections = nil

	log.Info("Unified connection manager closed")
	return nil
}

// GetStats 获取统计信息
func (m *UnifiedConnectionManager) GetStats() *UnifiedStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := &UnifiedStats{
		TotalConnections: len(m.connections),
		TotalBytesSent:   atomic.LoadInt64(&m.totalBytesSent),
		TotalBytesRecv:   atomic.LoadInt64(&m.totalBytesRecv),
		TotalErrors:      atomic.LoadInt64(&m.totalErrors),
		Connections:      make([]*ConnectionStats, 0, len(m.connections)),
	}

	var totalStreams int32
	var healthyConnections int

	for _, conn := range m.connections {
		connStats := &ConnectionStats{
			ID:          conn.ID,
			Status:      conn.GetStatus().String(),
			StreamCount: atomic.LoadInt32(&conn.StreamCount),
			BytesSent:   atomic.LoadInt64(&conn.BytesSent),
			BytesRecv:   atomic.LoadInt64(&conn.BytesRecv),
			ErrorCount:  atomic.LoadInt32(&conn.ErrorCount),
			CreatedAt:   conn.CreatedAt,
			LastUsed:    conn.LastUsed,
		}

		stats.Connections = append(stats.Connections, connStats)
		totalStreams += connStats.StreamCount

		if conn.IsHealthy() {
			healthyConnections++
		}
	}

	stats.TotalStreams = totalStreams
	stats.HealthyConnections = healthyConnections

	return stats
}

// maintenanceLoop 维护循环
func (m *UnifiedConnectionManager) maintenanceLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.performMaintenance()
		}
	}
}

// performMaintenance 执行维护任务
func (m *UnifiedConnectionManager) performMaintenance() {
	m.mu.Lock()
	defer m.mu.Unlock()

	healthyCount := 0
	for _, conn := range m.connections {
		if conn.IsHealthy() {
			healthyCount++
		} else {
			log.Warnf("Connection %s is unhealthy: %s", conn.ID, conn.GetStatus())
		}
	}

	log.Debugf("Maintenance: %d/%d connections healthy", healthyCount, len(m.connections))
}

// UnifiedStats 统一连接统计
type UnifiedStats struct {
	TotalConnections   int                `json:"total_connections"`
	HealthyConnections int                `json:"healthy_connections"`
	TotalStreams       int32              `json:"total_streams"`
	TotalBytesSent     int64              `json:"total_bytes_sent"`
	TotalBytesRecv     int64              `json:"total_bytes_recv"`
	TotalErrors        int64              `json:"total_errors"`
	Connections        []*ConnectionStats `json:"connections"`
}

// ConnectionStats 连接统计
type ConnectionStats struct {
	ID          string    `json:"id"`
	Status      string    `json:"status"`
	StreamCount int32     `json:"stream_count"`
	BytesSent   int64     `json:"bytes_sent"`
	BytesRecv   int64     `json:"bytes_recv"`
	ErrorCount  int32     `json:"error_count"`
	CreatedAt   time.Time `json:"created_at"`
	LastUsed    time.Time `json:"last_used"`
}
