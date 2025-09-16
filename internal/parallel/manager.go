package parallel

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rigel/pkg/log"
)

// TCPParallelConnection TCP并行连接管理器实现
type TCPParallelConnection struct {
	target      string
	config      *ConnectionConfig
	pool        ConnectionPool
	splitter    DataSplitter
	reassembler DataReassembler

	// 统计信息
	totalBytesSent   int64
	totalBytesRecv   int64
	connectionErrors int64

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.RWMutex
}

// NewTCPParallelConnection 创建TCP并行连接管理器
func NewTCPParallelConnection(target string, config *ConnectionConfig) *TCPParallelConnection {
	if config == nil {
		config = DefaultConnectionConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	// 创建连接池
	pool := NewTCPConnectionPool(target, config)

	// 创建数据分发器（默认使用轮询策略）
	splitter := NewDataSplitter(SplitStrategyRoundRobin)

	// 创建数据重组器
	reassembler := NewDataReassembler(true, config.BufferSize*10)

	return &TCPParallelConnection{
		target:      target,
		config:      config,
		pool:        pool,
		splitter:    splitter,
		reassembler: reassembler,
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Connect 建立并行连接
func (p *TCPParallelConnection) Connect(ctx context.Context, target string) error {
	log.Infof("Establishing parallel connections to %s", target)

	// 更新目标地址
	p.target = target

	// 启动连接池
	if err := p.pool.Start(ctx); err != nil {
		return fmt.Errorf("failed to start connection pool: %v", err)
	}

	log.Infof("Parallel connections established to %s", target)
	return nil
}

// Send 发送数据
func (p *TCPParallelConnection) Send(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty data")
	}

	// 获取可用连接
	connections := p.getAvailableConnections()
	if len(connections) == 0 {
		return fmt.Errorf("no available connections")
	}

	// 分发数据到多个连接
	segments, err := p.splitter.Split(data, connections)
	if err != nil {
		return fmt.Errorf("failed to split data: %v", err)
	}

	log.Debugf("Sending %d bytes across %d connections (%d segments)",
		len(data), len(connections), len(segments))

	// 并行发送数据段
	var wg sync.WaitGroup
	errChan := make(chan error, len(segments))

	for _, segment := range segments {
		wg.Add(1)
		go func(seg *DataSegment) {
			defer wg.Done()
			if err := p.sendSegment(seg, connections); err != nil {
				errChan <- err
			}
		}(segment)
	}

	wg.Wait()
	close(errChan)

	// 检查发送错误
	var sendErrors []error
	for err := range errChan {
		sendErrors = append(sendErrors, err)
	}

	if len(sendErrors) > 0 {
		atomic.AddInt64(&p.connectionErrors, int64(len(sendErrors)))
		return fmt.Errorf("send errors: %v", sendErrors[0])
	}

	atomic.AddInt64(&p.totalBytesSent, int64(len(data)))
	log.Debugf("Successfully sent %d bytes", len(data))

	return nil
}

// Receive 接收数据
func (p *TCPParallelConnection) Receive() ([]byte, error) {
	// 从所有连接接收数据段
	connections := p.getAvailableConnections()
	if len(connections) == 0 {
		return nil, fmt.Errorf("no available connections")
	}

	// 重置重组器
	p.reassembler.Reset()

	// 并行接收数据
	segmentChan := make(chan *DataSegment, len(connections)*10)
	errChan := make(chan error, len(connections))
	var wg sync.WaitGroup

	for _, conn := range connections {
		wg.Add(1)
		go func(c *ManagedConnection) {
			defer wg.Done()
			if err := p.receiveFromConnection(c, segmentChan); err != nil {
				errChan <- err
			}
		}(conn)
	}

	// 启动重组协程
	go func() {
		wg.Wait()
		close(segmentChan)
		close(errChan)
	}()

	// 处理接收到的数据段
	for segment := range segmentChan {
		if err := p.reassembler.AddSegment(segment); err != nil {
			log.Errorf("Failed to add segment to reassembler: %v", err)
			continue
		}

		// 检查是否完成重组
		if data, complete := p.reassembler.GetCompleteData(); complete {
			atomic.AddInt64(&p.totalBytesRecv, int64(len(data)))
			log.Debugf("Successfully received and reassembled %d bytes", len(data))
			return data, nil
		}
	}

	// 检查接收错误
	select {
	case err := <-errChan:
		if err != nil {
			atomic.AddInt64(&p.connectionErrors, 1)
			return nil, fmt.Errorf("receive error: %v", err)
		}
	default:
	}

	return nil, fmt.Errorf("failed to receive complete data")
}

// Close 关闭所有连接
func (p *TCPParallelConnection) Close() error {
	log.Info("Closing parallel connections")

	p.cancel()

	// 停止连接池
	if err := p.pool.Stop(); err != nil {
		log.Errorf("Error stopping connection pool: %v", err)
	}

	log.Info("Parallel connections closed")
	return nil
}

// GetStats 获取统计信息
func (p *TCPParallelConnection) GetStats() *ParallelStats {
	poolStats := p.pool.GetStats()

	return &ParallelStats{
		ConnectionCount:   poolStats.TotalConnections,
		TotalBytesSent:    atomic.LoadInt64(&p.totalBytesSent),
		TotalBytesRecv:    atomic.LoadInt64(&p.totalBytesRecv),
		AverageThroughput: p.calculateThroughput(),
		ConnectionErrors:  atomic.LoadInt64(&p.connectionErrors),
		Pool:              poolStats,
	}
}

// Scale 动态调整连接数
func (p *TCPParallelConnection) Scale(targetSize int) error {
	log.Infof("Scaling parallel connections to %d", targetSize)
	return p.pool.Scale(targetSize)
}

// getAvailableConnections 获取可用连接
func (p *TCPParallelConnection) getAvailableConnections() []*ManagedConnection {
	var connections []*ManagedConnection

	// 尝试获取多个连接
	maxConnections := p.config.MaxConnections
	for i := 0; i < maxConnections; i++ {
		conn, err := p.pool.GetConnection()
		if err != nil {
			break
		}
		connections = append(connections, conn)
	}

	return connections
}

// sendSegment 发送数据段
func (p *TCPParallelConnection) sendSegment(segment *DataSegment, connections []*ManagedConnection) error {
	// 找到对应的连接
	var targetConn *ManagedConnection
	for _, conn := range connections {
		if conn.ID == segment.ConnectionID {
			targetConn = conn
			break
		}
	}

	if targetConn == nil {
		return fmt.Errorf("connection %s not found", segment.ConnectionID)
	}

	// 发送数据
	_, err := targetConn.Conn.Write(segment.Data)
	if err != nil {
		targetConn.IncrementError()
		targetConn.SetStatus(ConnectionStatusError)
		return fmt.Errorf("failed to write to connection %s: %v", targetConn.ID, err)
	}

	// 更新连接统计
	targetConn.UpdateStats(int64(segment.Length), 0)

	// 归还连接
	p.pool.ReturnConnection(targetConn)

	return nil
}

// receiveFromConnection 从连接接收数据
func (p *TCPParallelConnection) receiveFromConnection(conn *ManagedConnection, segmentChan chan<- *DataSegment) error {
	defer p.pool.ReturnConnection(conn)

	buffer := make([]byte, p.config.BufferSize)

	// 设置读取超时
	conn.Conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer conn.Conn.SetReadDeadline(time.Time{})

	n, err := conn.Conn.Read(buffer)
	if err != nil {
		conn.IncrementError()
		conn.SetStatus(ConnectionStatusError)
		return fmt.Errorf("failed to read from connection %s: %v", conn.ID, err)
	}

	if n > 0 {
		// 创建数据段
		segment := &DataSegment{
			SequenceID:   uint64(time.Now().UnixNano()), // 简单的序列号生成
			ConnectionID: conn.ID,
			Data:         buffer[:n],
			Length:       n,
			Timestamp:    time.Now(),
			IsLast:       true, // 简化实现，每次读取都是完整数据
		}

		// 更新连接统计
		conn.UpdateStats(0, int64(n))

		// 发送到重组器
		select {
		case segmentChan <- segment:
		case <-p.ctx.Done():
			return fmt.Errorf("context cancelled")
		}
	}

	return nil
}

// calculateThroughput 计算平均吞吐量
func (p *TCPParallelConnection) calculateThroughput() float64 {
	totalBytes := atomic.LoadInt64(&p.totalBytesSent) + atomic.LoadInt64(&p.totalBytesRecv)

	// 简化计算：假设运行时间为1秒
	// 实际实现中应该记录开始时间
	throughputBps := float64(totalBytes * 8)        // 转换为bits
	throughputMbps := throughputBps / (1024 * 1024) // 转换为Mbps

	return throughputMbps
}

// SetSplitStrategy 设置数据分发策略
func (p *TCPParallelConnection) SetSplitStrategy(strategy SplitStrategy) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.splitter = NewDataSplitter(strategy)
	log.Infof("Changed split strategy to %s", strategy)
}

// GetSplitStrategy 获取当前分发策略
func (p *TCPParallelConnection) GetSplitStrategy() SplitStrategy {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.splitter.GetStrategy()
}
