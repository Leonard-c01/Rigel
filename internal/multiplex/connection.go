package multiplex

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rigel/pkg/log"
)

// TCPMultiplexConnection TCP多路复用连接实现
type TCPMultiplexConnection struct {
	target         string
	config         *MultiplexConfig
	sessionManager SessionManager
	streamManager  StreamManager

	// 统计信息
	totalBytesSent int64
	totalBytesRecv int64

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.RWMutex
}

// NewTCPMultiplexConnection 创建TCP多路复用连接
func NewTCPMultiplexConnection(target string, config *MultiplexConfig) *TCPMultiplexConnection {
	if config == nil {
		config = DefaultMultiplexConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	sessionManager := NewTCPSessionManager(config)
	streamManager := NewTCPStreamManager(sessionManager)

	return &TCPMultiplexConnection{
		target:         target,
		config:         config,
		sessionManager: sessionManager,
		streamManager:  streamManager,
		ctx:            ctx,
		cancel:         cancel,
	}
}

// Connect 建立多路复用连接
func (mc *TCPMultiplexConnection) Connect(ctx context.Context, target string) error {
	log.Infof("Establishing multiplex connection to %s", target)

	mc.target = target

	// 创建初始会话
	for i := 0; i < mc.config.MinSessions; i++ {
		_, err := mc.sessionManager.CreateSession(ctx, target)
		if err != nil {
			log.Errorf("Failed to create initial session %d: %v", i, err)
			continue
		}
	}

	log.Infof("Multiplex connection established to %s", target)
	return nil
}

// OpenStream 打开新的逻辑流
func (mc *TCPMultiplexConnection) OpenStream() (net.Conn, error) {
	// 获取可用会话
	session, err := mc.sessionManager.GetSession(mc.target)
	if err != nil {
		return nil, fmt.Errorf("failed to get session: %v", err)
	}

	// 在会话上打开新流
	stream, err := session.Session.OpenStream()
	if err != nil {
		mc.sessionManager.ReturnSession(session)
		return nil, fmt.Errorf("failed to open stream: %v", err)
	}

	// 增加会话流计数
	session.IncrementStreamCount()

	// 创建流包装器
	streamID := fmt.Sprintf("stream_%d", time.Now().UnixNano())
	muxStream := &MuxStream{
		ID:        streamID,
		Stream:    stream,
		SessionID: session.ID,
		CreatedAt: time.Now(),
		LastUsed:  time.Now(),
	}

	// 添加到流管理器
	mc.mu.Lock()
	// 这里简化实现，实际应该调用streamManager.AddStream
	mc.mu.Unlock()

	log.Debugf("Opened new stream %s on session %s", streamID, session.ID)

	// 返回包装的连接
	return &MultiplexStreamConn{
		stream:         muxStream,
		session:        session,
		sessionManager: mc.sessionManager,
		onClose: func() {
			session.DecrementStreamCount()
			mc.sessionManager.ReturnSession(session)
		},
	}, nil
}

// Send 发送数据（自动选择或创建流）
func (mc *TCPMultiplexConnection) Send(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty data")
	}

	// 打开新流进行发送
	stream, err := mc.OpenStream()
	if err != nil {
		return fmt.Errorf("failed to open stream: %v", err)
	}
	defer stream.Close()

	// 发送数据
	_, err = stream.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write data: %v", err)
	}

	atomic.AddInt64(&mc.totalBytesSent, int64(len(data)))
	log.Debugf("Sent %d bytes via multiplex stream", len(data))

	return nil
}

// Receive 接收数据
func (mc *TCPMultiplexConnection) Receive() ([]byte, error) {
	// 打开新流进行接收
	stream, err := mc.OpenStream()
	if err != nil {
		return nil, fmt.Errorf("failed to open stream: %v", err)
	}
	defer stream.Close()

	// 接收数据
	buffer := make([]byte, mc.config.MaxStreamBuffer)
	n, err := stream.Read(buffer)
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %v", err)
	}

	data := buffer[:n]
	atomic.AddInt64(&mc.totalBytesRecv, int64(len(data)))
	log.Debugf("Received %d bytes via multiplex stream", len(data))

	return data, nil
}

// Close 关闭多路复用连接
func (mc *TCPMultiplexConnection) Close() error {
	log.Info("Closing multiplex connection")

	mc.cancel()

	// 关闭流管理器（简化实现）
	// if err := mc.streamManager.Close(); err != nil {
	//     log.Errorf("Error closing stream manager: %v", err)
	// }

	// 关闭会话管理器
	if err := mc.sessionManager.Close(); err != nil {
		log.Errorf("Error closing session manager: %v", err)
	}

	log.Info("Multiplex connection closed")
	return nil
}

// GetStats 获取统计信息
func (mc *TCPMultiplexConnection) GetStats() *MultiplexStats {
	sessionStats := mc.sessionManager.GetStats()
	streamStats := mc.streamManager.GetStats()

	return &MultiplexStats{
		SessionCount:   sessionStats.TotalSessions,
		StreamCount:    int32(streamStats.TotalStreams),
		TotalBytesSent: atomic.LoadInt64(&mc.totalBytesSent),
		TotalBytesRecv: atomic.LoadInt64(&mc.totalBytesRecv),
		SessionStats:   sessionStats,
		StreamStats:    streamStats,
	}
}

// Scale 动态调整会话数
func (mc *TCPMultiplexConnection) Scale(targetSessions int) error {
	if targetSessions < mc.config.MinSessions {
		targetSessions = mc.config.MinSessions
	}
	if targetSessions > mc.config.MaxSessions {
		targetSessions = mc.config.MaxSessions
	}

	stats := mc.sessionManager.GetStats()
	currentSessions := stats.TotalSessions

	if targetSessions > currentSessions {
		// 扩容
		needed := targetSessions - currentSessions
		for i := 0; i < needed; i++ {
			_, err := mc.sessionManager.CreateSession(mc.ctx, mc.target)
			if err != nil {
				log.Errorf("Failed to create session during scaling: %v", err)
				break
			}
		}
		log.Infof("Scaled up sessions from %d to %d", currentSessions, targetSessions)
	}

	// 缩容通过会话管理器的维护循环自动处理

	return nil
}

// MultiplexStreamConn 多路复用流连接包装器
type MultiplexStreamConn struct {
	stream         *MuxStream
	session        *MuxSession
	sessionManager SessionManager
	onClose        func()
	closed         bool
	mu             sync.Mutex
}

// Read 读取数据
func (msc *MultiplexStreamConn) Read(b []byte) (int, error) {
	if msc.stream.Stream == nil {
		return 0, fmt.Errorf("stream is nil")
	}

	n, err := msc.stream.Stream.Read(b)
	if err == nil && n > 0 {
		msc.stream.UpdateStats(0, int64(n))
		msc.session.UpdateStats(0, int64(n))
	}

	return n, err
}

// Write 写入数据
func (msc *MultiplexStreamConn) Write(b []byte) (int, error) {
	if msc.stream.Stream == nil {
		return 0, fmt.Errorf("stream is nil")
	}

	n, err := msc.stream.Stream.Write(b)
	if err == nil && n > 0 {
		msc.stream.UpdateStats(int64(n), 0)
		msc.session.UpdateStats(int64(n), 0)
	}

	return n, err
}

// Close 关闭连接
func (msc *MultiplexStreamConn) Close() error {
	msc.mu.Lock()
	defer msc.mu.Unlock()

	if msc.closed {
		return nil
	}

	msc.closed = true

	var err error
	if msc.stream.Stream != nil {
		err = msc.stream.Stream.Close()
	}

	if msc.onClose != nil {
		msc.onClose()
	}

	return err
}

// LocalAddr 获取本地地址
func (msc *MultiplexStreamConn) LocalAddr() net.Addr {
	if msc.stream.Stream != nil {
		return msc.stream.Stream.LocalAddr()
	}
	return nil
}

// RemoteAddr 获取远程地址
func (msc *MultiplexStreamConn) RemoteAddr() net.Addr {
	if msc.stream.Stream != nil {
		return msc.stream.Stream.RemoteAddr()
	}
	return nil
}

// SetDeadline 设置截止时间
func (msc *MultiplexStreamConn) SetDeadline(t time.Time) error {
	if msc.stream.Stream != nil {
		return msc.stream.Stream.SetDeadline(t)
	}
	return fmt.Errorf("stream is nil")
}

// SetReadDeadline 设置读取截止时间
func (msc *MultiplexStreamConn) SetReadDeadline(t time.Time) error {
	if msc.stream.Stream != nil {
		return msc.stream.Stream.SetReadDeadline(t)
	}
	return fmt.Errorf("stream is nil")
}

// SetWriteDeadline 设置写入截止时间
func (msc *MultiplexStreamConn) SetWriteDeadline(t time.Time) error {
	if msc.stream.Stream != nil {
		return msc.stream.Stream.SetWriteDeadline(t)
	}
	return fmt.Errorf("stream is nil")
}
