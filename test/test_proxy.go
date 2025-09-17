package test

import (
	"fmt"
	"net"
	"time"

	"github.com/rigel/config"
	"github.com/rigel/internal/dataplane"
	"github.com/rigel/pkg/log"
)

// TestProxy 测试用的简化代理
type TestProxy struct {
	config    *config.Config
	forwarder *dataplane.DataPlaneForwarder
	running   bool
}

// NewTestProxy 创建测试代理
func NewTestProxy(cfg *config.Config) *TestProxy {
	// 从配置中获取监听地址
	listenAddr := ":8080"
	if len(cfg.Listeners) > 0 {
		listenAddr = cfg.Listeners[0].Bind
	}

	// 创建数据面转发器
	nodeAddress := cfg.Common.NodeID + listenAddr
	forwarder := dataplane.NewDataPlaneForwarder(nodeAddress, listenAddr)

	return &TestProxy{
		config:    cfg,
		forwarder: forwarder,
		running:   false,
	}
}

// Start 启动代理
func (tp *TestProxy) Start() error {
	if tp.running {
		return fmt.Errorf("proxy already running")
	}

	// 启动数据面转发器
	if err := tp.forwarder.Start(); err != nil {
		return fmt.Errorf("failed to start forwarder: %v", err)
	}

	tp.running = true
	log.Infof("Test proxy started: %s", tp.config.Common.NodeID)
	return nil
}

// Stop 停止代理
func (tp *TestProxy) Stop() error {
	if !tp.running {
		return nil
	}

	// 停止数据面转发器
	if err := tp.forwarder.Stop(); err != nil {
		return fmt.Errorf("failed to stop forwarder: %v", err)
	}

	tp.running = false
	log.Infof("Test proxy stopped: %s", tp.config.Common.NodeID)
	return nil
}

// GetStats 获取统计信息
func (tp *TestProxy) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})

	stats["node_id"] = tp.config.Common.NodeID
	stats["running"] = tp.running
	stats["scheduler_connected"] = false // 测试环境下调度器通常不连接

	if tp.forwarder != nil {
		forwarderStats := tp.forwarder.GetStats()
		stats["total_connections"] = forwarderStats.TotalConnections
		stats["active_connections"] = forwarderStats.ActiveConnections
		stats["total_forwards"] = forwarderStats.TotalForwards
		stats["total_bytes"] = forwarderStats.TotalBytes
		stats["error_count"] = forwarderStats.ErrorCount
	}

	return stats
}

// IsRunning 检查是否运行中
func (tp *TestProxy) IsRunning() bool {
	return tp.running
}

// TestEchoServer 测试用的回显服务器
type TestEchoServer struct {
	listener net.Listener
	running  bool
}

// NewTestEchoServer 创建测试回显服务器
func NewTestEchoServer(addr string) (*TestEchoServer, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %v", addr, err)
	}

	server := &TestEchoServer{
		listener: listener,
		running:  true,
	}

	// 启动服务器
	go server.serve()

	return server, nil
}

// serve 服务器主循环
func (tes *TestEchoServer) serve() {
	for tes.running {
		conn, err := tes.listener.Accept()
		if err != nil {
			if tes.running {
				log.Errorf("Accept error: %v", err)
			}
			continue
		}

		go tes.handleConnection(conn)
	}
}

// handleConnection 处理连接
func (tes *TestEchoServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, 64*1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			break
		}

		data := buffer[:n]
		response := fmt.Sprintf("Echo: %s", string(data))
		conn.Write([]byte(response))
	}
}

// Close 关闭服务器
func (tes *TestEchoServer) Close() error {
	tes.running = false
	if tes.listener != nil {
		return tes.listener.Close()
	}
	return nil
}

// Addr 获取监听地址
func (tes *TestEchoServer) Addr() net.Addr {
	if tes.listener != nil {
		return tes.listener.Addr()
	}
	return nil
}

// TestConnection 测试连接辅助函数
func TestConnection(addr, message string) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return "", fmt.Errorf("failed to connect: %v", err)
	}
	defer conn.Close()

	// 发送消息
	_, err = conn.Write([]byte(message))
	if err != nil {
		return "", fmt.Errorf("failed to write: %v", err)
	}

	// 读取响应
	buffer := make([]byte, 64*1024)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := conn.Read(buffer)
	if err != nil {
		return "", fmt.Errorf("failed to read: %v", err)
	}

	return string(buffer[:n]), nil
}
