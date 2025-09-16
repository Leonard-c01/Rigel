package multiplex

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/rigel/pkg/log"
)

// MultiplexTransport 多路复用传输器
type MultiplexTransport struct {
	connection MultiplexConnection
	config     *MultiplexConfig

	// 流池管理
	streamPool    chan net.Conn
	activeStreams map[string]net.Conn
	mu            sync.RWMutex

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewMultiplexTransport 创建多路复用传输器
func NewMultiplexTransport(target string, config *MultiplexConfig) *MultiplexTransport {
	if config == nil {
		config = DefaultMultiplexConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())
	connection := NewTCPMultiplexConnection(target, config)

	return &MultiplexTransport{
		connection:    connection,
		config:        config,
		streamPool:    make(chan net.Conn, config.MaxStreamsPerSession),
		activeStreams: make(map[string]net.Conn),
		ctx:           ctx,
		cancel:        cancel,
	}
}

// Connect 建立传输连接
func (mt *MultiplexTransport) Connect(ctx context.Context, target string) error {
	log.Infof("Connecting multiplex transport to %s", target)

	// 建立多路复用连接
	if err := mt.connection.Connect(ctx, target); err != nil {
		return fmt.Errorf("failed to connect: %v", err)
	}

	// 预创建流池
	mt.wg.Add(1)
	go mt.streamPoolManager()

	log.Infof("Multiplex transport connected to %s", target)
	return nil
}

// SendData 发送数据
func (mt *MultiplexTransport) SendData(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty data")
	}

	// 从流池获取或创建新流
	stream, err := mt.getOrCreateStream()
	if err != nil {
		return fmt.Errorf("failed to get stream: %v", err)
	}

	// 发送数据
	_, err = stream.Write(data)
	if err != nil {
		// 发送失败，关闭流
		stream.Close()
		return fmt.Errorf("failed to write data: %v", err)
	}

	// 归还流到池中（如果需要复用）
	mt.returnStream(stream)

	log.Debugf("Sent %d bytes via multiplex transport", len(data))
	return nil
}

// ReceiveData 接收数据
func (mt *MultiplexTransport) ReceiveData() ([]byte, error) {
	// 从流池获取或创建新流
	stream, err := mt.getOrCreateStream()
	if err != nil {
		return nil, fmt.Errorf("failed to get stream: %v", err)
	}

	// 设置读取超时
	stream.SetReadDeadline(time.Now().Add(mt.config.StreamTimeout))
	defer stream.SetReadDeadline(time.Time{})

	// 接收数据
	buffer := make([]byte, mt.config.MaxStreamBuffer)
	n, err := stream.Read(buffer)
	if err != nil {
		stream.Close()
		return nil, fmt.Errorf("failed to read data: %v", err)
	}

	data := buffer[:n]

	// 归还流到池中
	mt.returnStream(stream)

	log.Debugf("Received %d bytes via multiplex transport", len(data))
	return data, nil
}

// ForwardData 双向数据转发
func (mt *MultiplexTransport) ForwardData(clientConn net.Conn) (int64, int64, error) {
	// 获取多路复用流
	serverStream, err := mt.getOrCreateStream()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get server stream: %v", err)
	}
	defer serverStream.Close()

	// 双向数据转发
	var wg sync.WaitGroup
	var clientToServer, serverToClient int64
	var forwardErr error

	// 客户端到服务器
	wg.Add(1)
	go func() {
		defer wg.Done()
		n, err := io.Copy(serverStream, clientConn)
		clientToServer = n
		if err != nil && forwardErr == nil {
			forwardErr = fmt.Errorf("client to server copy error: %v", err)
		}
		// 关闭写入方向
		if closer, ok := serverStream.(interface{ CloseWrite() error }); ok {
			closer.CloseWrite()
		}
	}()

	// 服务器到客户端
	wg.Add(1)
	go func() {
		defer wg.Done()
		n, err := io.Copy(clientConn, serverStream)
		serverToClient = n
		if err != nil && forwardErr == nil {
			forwardErr = fmt.Errorf("server to client copy error: %v", err)
		}
		// 关闭写入方向
		if closer, ok := clientConn.(interface{ CloseWrite() error }); ok {
			closer.CloseWrite()
		}
	}()

	wg.Wait()

	log.Debugf("Forwarded data: client->server=%d bytes, server->client=%d bytes",
		clientToServer, serverToClient)

	return clientToServer, serverToClient, forwardErr
}

// Close 关闭传输器
func (mt *MultiplexTransport) Close() error {
	log.Info("Closing multiplex transport")

	mt.cancel()
	mt.wg.Wait()

	// 关闭所有活跃流
	mt.mu.Lock()
	for streamID, stream := range mt.activeStreams {
		stream.Close()
		delete(mt.activeStreams, streamID)
	}
	mt.mu.Unlock()

	// 关闭流池
	close(mt.streamPool)
	for stream := range mt.streamPool {
		stream.Close()
	}

	// 关闭多路复用连接
	if err := mt.connection.Close(); err != nil {
		log.Errorf("Error closing multiplex connection: %v", err)
	}

	log.Info("Multiplex transport closed")
	return nil
}

// GetStats 获取传输统计
func (mt *MultiplexTransport) GetStats() *MultiplexStats {
	return mt.connection.GetStats()
}

// getOrCreateStream 获取或创建流
func (mt *MultiplexTransport) getOrCreateStream() (net.Conn, error) {
	// 尝试从池中获取
	select {
	case stream := <-mt.streamPool:
		if stream != nil {
			return stream, nil
		}
	default:
		// 池为空，创建新流
	}

	// 创建新流
	stream, err := mt.connection.OpenStream()
	if err != nil {
		return nil, fmt.Errorf("failed to open stream: %v", err)
	}

	// 添加到活跃流映射
	streamID := fmt.Sprintf("stream_%d", time.Now().UnixNano())
	mt.mu.Lock()
	mt.activeStreams[streamID] = stream
	mt.mu.Unlock()

	return stream, nil
}

// returnStream 归还流到池中
func (mt *MultiplexTransport) returnStream(stream net.Conn) {
	if stream == nil {
		return
	}

	// 检查流是否仍然可用
	// 这里简化实现，实际应该检查连接状态

	select {
	case mt.streamPool <- stream:
		// 成功归还到池中
	default:
		// 池满，直接关闭流
		stream.Close()
	}
}

// streamPoolManager 流池管理器
func (mt *MultiplexTransport) streamPoolManager() {
	defer mt.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-mt.ctx.Done():
			return
		case <-ticker.C:
			mt.cleanupStreams()
		}
	}
}

// cleanupStreams 清理无效流
func (mt *MultiplexTransport) cleanupStreams() {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	// 清理活跃流映射中的无效流
	for streamID, stream := range mt.activeStreams {
		// 检查流是否仍然有效（这里简化实现）
		if stream == nil {
			delete(mt.activeStreams, streamID)
		}
	}

	// 清理流池中的无效流
	poolSize := len(mt.streamPool)
	for i := 0; i < poolSize; i++ {
		select {
		case stream := <-mt.streamPool:
			if stream != nil {
				// 检查流是否有效，如果有效则放回池中
				select {
				case mt.streamPool <- stream:
				default:
					stream.Close()
				}
			}
		default:
			break
		}
	}

	log.Debugf("Stream cleanup completed, active streams: %d, pool size: %d",
		len(mt.activeStreams), len(mt.streamPool))
}

// Scale 动态调整传输能力
func (mt *MultiplexTransport) Scale(targetSessions int) error {
	return mt.connection.Scale(targetSessions)
}

// GetConnectionMode 获取连接模式
func (mt *MultiplexTransport) GetConnectionMode() ConnectionMode {
	return ModeMultiplexOnly
}
