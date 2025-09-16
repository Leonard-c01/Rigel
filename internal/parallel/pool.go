package parallel

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/rigel/pkg/log"
)

// TCPConnectionPool TCP连接池实现
type TCPConnectionPool struct {
	target      string
	config      *ConnectionConfig
	connections map[string]*ManagedConnection
	available   chan *ManagedConnection
	mu          sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	stats       *PoolStats
}

// NewTCPConnectionPool 创建TCP连接池
func NewTCPConnectionPool(target string, config *ConnectionConfig) *TCPConnectionPool {
	if config == nil {
		config = DefaultConnectionConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &TCPConnectionPool{
		target:      target,
		config:      config,
		connections: make(map[string]*ManagedConnection),
		available:   make(chan *ManagedConnection, config.MaxConnections),
		ctx:         ctx,
		cancel:      cancel,
		stats: &PoolStats{
			TotalConnections:  0,
			ActiveConnections: 0,
			IdleConnections:   0,
			ErrorConnections:  0,
		},
	}
}

// Start 启动连接池
func (p *TCPConnectionPool) Start(ctx context.Context) error {
	log.Infof("Starting TCP connection pool for target %s", p.target)

	// 创建初始连接
	for i := 0; i < p.config.InitialConnections; i++ {
		if err := p.createConnection(); err != nil {
			log.Errorf("Failed to create initial connection %d: %v", i, err)
			continue
		}
	}

	// 启动健康检查
	p.wg.Add(1)
	go p.healthCheckLoop()

	// 启动连接维护
	p.wg.Add(1)
	go p.maintenanceLoop()

	log.Infof("TCP connection pool started with %d initial connections", len(p.connections))
	return nil
}

// Stop 停止连接池
func (p *TCPConnectionPool) Stop() error {
	log.Info("Stopping TCP connection pool")

	p.cancel()
	p.wg.Wait()

	// 关闭所有连接
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, conn := range p.connections {
		if conn.Conn != nil {
			conn.Conn.Close()
		}
		conn.SetStatus(ConnectionStatusClosed)
	}

	close(p.available)
	log.Info("TCP connection pool stopped")
	return nil
}

// GetConnection 获取可用连接
func (p *TCPConnectionPool) GetConnection() (*ManagedConnection, error) {
	select {
	case conn := <-p.available:
		if conn.IsHealthy() {
			conn.SetStatus(ConnectionStatusActive)
			p.updateStats()
			return conn, nil
		}
		// 连接不健康，尝试重新创建
		p.removeConnection(conn.ID)
		return p.GetConnection()

	case <-time.After(p.config.ConnectTimeout):
		// 超时，尝试创建新连接
		if p.canCreateConnection() {
			if err := p.createConnection(); err != nil {
				return nil, fmt.Errorf("failed to create new connection: %v", err)
			}
			return p.GetConnection()
		}
		return nil, fmt.Errorf("connection pool timeout")

	case <-p.ctx.Done():
		return nil, fmt.Errorf("connection pool stopped")
	}
}

// ReturnConnection 归还连接
func (p *TCPConnectionPool) ReturnConnection(conn *ManagedConnection) {
	if conn == nil {
		return
	}

	if !conn.IsHealthy() {
		p.removeConnection(conn.ID)
		return
	}

	conn.SetStatus(ConnectionStatusIdle)

	select {
	case p.available <- conn:
		// 成功归还
	default:
		// 队列满，关闭连接
		p.removeConnection(conn.ID)
	}

	p.updateStats()
}

// GetStats 获取连接池统计
func (p *TCPConnectionPool) GetStats() *PoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := &PoolStats{
		TotalConnections:  len(p.connections),
		ActiveConnections: 0,
		IdleConnections:   0,
		ErrorConnections:  0,
		TotalBytesSent:    0,
		TotalBytesRecv:    0,
		ConnectionErrors:  0,
	}

	for _, conn := range p.connections {
		switch conn.GetStatus() {
		case ConnectionStatusActive:
			stats.ActiveConnections++
		case ConnectionStatusIdle:
			stats.IdleConnections++
		case ConnectionStatusError:
			stats.ErrorConnections++
		}

		stats.TotalBytesSent += conn.BytesSent
		stats.TotalBytesRecv += conn.BytesRecv
		stats.ConnectionErrors += int64(conn.ErrorCount)
	}

	return stats
}

// Scale 动态调整连接数
func (p *TCPConnectionPool) Scale(targetSize int) error {
	if targetSize < p.config.MinConnections {
		targetSize = p.config.MinConnections
	}
	if targetSize > p.config.MaxConnections {
		targetSize = p.config.MaxConnections
	}

	p.mu.RLock()
	currentSize := len(p.connections)
	p.mu.RUnlock()

	if targetSize > currentSize {
		// 扩容
		for i := currentSize; i < targetSize; i++ {
			if err := p.createConnection(); err != nil {
				log.Errorf("Failed to create connection during scaling: %v", err)
				break
			}
		}
		log.Infof("Scaled up connection pool from %d to %d", currentSize, targetSize)
	} else if targetSize < currentSize {
		// 缩容
		toRemove := currentSize - targetSize
		p.removeIdleConnections(toRemove)
		log.Infof("Scaled down connection pool from %d to %d", currentSize, targetSize)
	}

	return nil
}

// createConnection 创建新连接
func (p *TCPConnectionPool) createConnection() error {
	if !p.canCreateConnection() {
		return fmt.Errorf("cannot create more connections (max: %d)", p.config.MaxConnections)
	}

	conn, err := net.DialTimeout("tcp", p.target, p.config.ConnectTimeout)
	if err != nil {
		return fmt.Errorf("failed to dial %s: %v", p.target, err)
	}

	id := p.generateConnectionID()
	managedConn := &ManagedConnection{
		ID:        id,
		Conn:      conn,
		Status:    ConnectionStatusIdle,
		CreatedAt: time.Now(),
		LastUsed:  time.Now(),
	}

	p.mu.Lock()
	p.connections[id] = managedConn
	p.mu.Unlock()

	// 添加到可用队列
	select {
	case p.available <- managedConn:
		log.Debugf("Created new connection %s to %s", id, p.target)
	default:
		// 队列满，移除连接
		p.removeConnection(id)
		return fmt.Errorf("available queue full")
	}

	return nil
}

// removeConnection 移除连接
func (p *TCPConnectionPool) removeConnection(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if conn, exists := p.connections[id]; exists {
		if conn.Conn != nil {
			conn.Conn.Close()
		}
		conn.SetStatus(ConnectionStatusClosed)
		delete(p.connections, id)
		log.Debugf("Removed connection %s", id)
	}
}

// canCreateConnection 检查是否可以创建新连接
func (p *TCPConnectionPool) canCreateConnection() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.connections) < p.config.MaxConnections
}

// generateConnectionID 生成连接ID
func (p *TCPConnectionPool) generateConnectionID() string {
	bytes := make([]byte, 8)
	rand.Read(bytes)
	return fmt.Sprintf("conn_%x", bytes)
}

// healthCheckLoop 健康检查循环
func (p *TCPConnectionPool) healthCheckLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.performHealthCheck()
		}
	}
}

// performHealthCheck 执行健康检查
func (p *TCPConnectionPool) performHealthCheck() {
	p.mu.RLock()
	connections := make([]*ManagedConnection, 0, len(p.connections))
	for _, conn := range p.connections {
		connections = append(connections, conn)
	}
	p.mu.RUnlock()

	for _, conn := range connections {
		if !p.isConnectionAlive(conn) {
			log.Debugf("Connection %s failed health check", conn.ID)
			p.removeConnection(conn.ID)
		}
	}
}

// isConnectionAlive 检查连接是否存活
func (p *TCPConnectionPool) isConnectionAlive(conn *ManagedConnection) bool {
	if conn.Conn == nil {
		return false
	}

	// 设置短超时进行读取测试
	conn.Conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	defer conn.Conn.SetReadDeadline(time.Time{})

	// 尝试读取一个字节（非阻塞）
	buffer := make([]byte, 1)
	_, err := conn.Conn.Read(buffer)

	// 如果是超时错误，说明连接正常但没有数据
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}

	// 其他错误说明连接有问题
	return err == nil
}

// maintenanceLoop 连接维护循环
func (p *TCPConnectionPool) maintenanceLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.performMaintenance()
		}
	}
}

// performMaintenance 执行连接维护
func (p *TCPConnectionPool) performMaintenance() {
	// 移除空闲超时的连接
	p.removeIdleTimeoutConnections()

	// 确保最小连接数
	p.ensureMinConnections()

	// 更新统计信息
	p.updateStats()
}

// removeIdleTimeoutConnections 移除空闲超时的连接
func (p *TCPConnectionPool) removeIdleTimeoutConnections() {
	p.mu.RLock()
	toRemove := make([]string, 0)
	now := time.Now()

	for id, conn := range p.connections {
		if conn.GetStatus() == ConnectionStatusIdle &&
			now.Sub(conn.LastUsed) > p.config.IdleTimeout {
			toRemove = append(toRemove, id)
		}
	}
	p.mu.RUnlock()

	for _, id := range toRemove {
		p.removeConnection(id)
		log.Debugf("Removed idle timeout connection %s", id)
	}
}

// removeIdleConnections 移除指定数量的空闲连接
func (p *TCPConnectionPool) removeIdleConnections(count int) {
	p.mu.RLock()
	toRemove := make([]string, 0, count)

	for id, conn := range p.connections {
		if len(toRemove) >= count {
			break
		}
		if conn.GetStatus() == ConnectionStatusIdle {
			toRemove = append(toRemove, id)
		}
	}
	p.mu.RUnlock()

	for _, id := range toRemove {
		p.removeConnection(id)
	}
}

// ensureMinConnections 确保最小连接数
func (p *TCPConnectionPool) ensureMinConnections() {
	p.mu.RLock()
	currentSize := len(p.connections)
	p.mu.RUnlock()

	if currentSize < p.config.MinConnections {
		needed := p.config.MinConnections - currentSize
		for i := 0; i < needed; i++ {
			if err := p.createConnection(); err != nil {
				log.Errorf("Failed to create connection for min requirement: %v", err)
				break
			}
		}
	}
}

// updateStats 更新统计信息
func (p *TCPConnectionPool) updateStats() {
	// 统计信息在GetStats中实时计算，这里可以做一些缓存优化
}
