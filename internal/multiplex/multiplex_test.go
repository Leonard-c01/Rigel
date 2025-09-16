package multiplex

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/rigel/pkg/log"
)

func init() {
	log.Init("debug", "")
}

func TestTCPSessionManager(t *testing.T) {
	// 启动测试服务器
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer listener.Close()

	target := listener.Addr().String()

	// 启动简单的回显服务器
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buffer := make([]byte, 1024)
				n, _ := c.Read(buffer)
				c.Write(buffer[:n])
			}(conn)
		}
	}()

	// 创建会话管理器
	config := DefaultMultiplexConfig()
	config.MinSessions = 1
	config.MaxSessions = 3

	sessionManager := NewTCPSessionManager(config)
	defer sessionManager.Close()

	// 测试创建会话
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	session1, err := sessionManager.CreateSession(ctx, target)
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	if session1 == nil {
		t.Fatal("Got nil session")
	}

	if session1.Target != target {
		t.Errorf("Expected target %s, got %s", target, session1.Target)
	}

	// 测试获取会话
	session2, err := sessionManager.GetSession(target)
	if err != nil {
		t.Fatalf("Failed to get session: %v", err)
	}

	// 应该返回同一个会话（因为流数未达到上限）
	if session2.ID != session1.ID {
		t.Errorf("Expected same session ID %s, got %s", session1.ID, session2.ID)
	}

	// 测试会话统计
	stats := sessionManager.GetStats()
	if stats.TotalSessions < 1 {
		t.Errorf("Expected at least 1 session, got %d", stats.TotalSessions)
	}

	// 归还会话
	sessionManager.ReturnSession(session1)
	sessionManager.ReturnSession(session2)

	t.Logf("Session manager stats: %+v", stats)
}

func TestTCPMultiplexConnection(t *testing.T) {
	// 启动测试服务器
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer listener.Close()

	target := listener.Addr().String()

	// 启动smux服务器
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleSmuxServer(conn)
		}
	}()

	// 创建多路复用连接
	config := DefaultMultiplexConfig()
	config.MinSessions = 1
	config.MaxSessions = 2

	multiplexConn := NewTCPMultiplexConnection(target, config)

	// 建立连接
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := multiplexConn.Connect(ctx, target); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer multiplexConn.Close()

	// 等待连接建立
	time.Sleep(100 * time.Millisecond)

	// 测试打开流
	stream, err := multiplexConn.OpenStream()
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	// 测试发送数据
	testData := []byte("Hello, Multiplex!")
	_, err = stream.Write(testData)
	if err != nil {
		t.Fatalf("Failed to write to stream: %v", err)
	}

	// 测试接收数据
	buffer := make([]byte, 1024)
	n, err := stream.Read(buffer)
	if err != nil {
		t.Fatalf("Failed to read from stream: %v", err)
	}

	response := buffer[:n]
	if string(response) != string(testData) {
		t.Errorf("Expected %q, got %q", string(testData), string(response))
	}

	// 检查统计信息
	stats := multiplexConn.GetStats()
	if stats.SessionCount < 1 {
		t.Errorf("Expected at least 1 session, got %d", stats.SessionCount)
	}

	t.Logf("Multiplex connection stats: %+v", stats)
}

func TestMultiplexTransport(t *testing.T) {
	// 启动测试服务器
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer listener.Close()

	target := listener.Addr().String()

	// 启动smux服务器
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleSmuxServer(conn)
		}
	}()

	// 创建多路复用传输器
	config := DefaultMultiplexConfig()
	transport := NewMultiplexTransport(target, config)

	// 建立连接
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := transport.Connect(ctx, target); err != nil {
		t.Fatalf("Failed to connect transport: %v", err)
	}
	defer transport.Close()

	// 等待连接建立
	time.Sleep(100 * time.Millisecond)

	// 测试发送数据
	testData := []byte("Hello, Transport!")
	err = transport.SendData(testData)
	if err != nil {
		t.Fatalf("Failed to send data: %v", err)
	}

	// 测试接收数据
	receivedData, err := transport.ReceiveData()
	if err != nil {
		t.Fatalf("Failed to receive data: %v", err)
	}

	if string(receivedData) != string(testData) {
		t.Errorf("Expected %q, got %q", string(testData), string(receivedData))
	}

	// 检查统计信息
	stats := transport.GetStats()
	if stats.SessionCount < 1 {
		t.Errorf("Expected at least 1 session, got %d", stats.SessionCount)
	}

	t.Logf("Transport stats: %+v", stats)
}

func TestHybridConnection(t *testing.T) {
	// 启动测试服务器
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer listener.Close()

	target := listener.Addr().String()

	// 启动混合服务器（支持普通TCP和smux）
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleHybridServer(conn)
		}
	}()

	// 创建混合连接
	config := DefaultHybridConnectionConfig()
	config.Mode = ModeAuto

	hybridConn := NewHybridConnection(target, config)

	// 建立连接
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := hybridConn.Connect(ctx, target); err != nil {
		t.Fatalf("Failed to connect hybrid: %v", err)
	}
	defer hybridConn.Close()

	// 等待连接建立
	time.Sleep(200 * time.Millisecond)

	// 测试发送数据
	testData := []byte("Hello, Hybrid Connection!")
	err = hybridConn.Send(testData)
	if err != nil {
		t.Fatalf("Failed to send data: %v", err)
	}

	// 测试接收数据
	receivedData, err := hybridConn.Receive()
	if err != nil {
		t.Fatalf("Failed to receive data: %v", err)
	}

	if string(receivedData) != string(testData) {
		t.Errorf("Expected %q, got %q", string(testData), string(receivedData))
	}

	// 检查统计信息
	stats := hybridConn.GetStats()
	if stats.TotalBytesSent == 0 {
		t.Error("Expected bytes sent > 0")
	}

	t.Logf("Hybrid connection stats: %+v", stats)
}

// handleSmuxServer 处理smux服务器连接
func handleSmuxServer(conn net.Conn) {
	defer conn.Close()

	// 简化实现：直接回显数据
	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			break
		}
		conn.Write(buffer[:n])
	}
}

// handleHybridServer 处理混合服务器连接
func handleHybridServer(conn net.Conn) {
	defer conn.Close()

	// 简化实现：直接回显数据
	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			break
		}
		conn.Write(buffer[:n])
	}
}
