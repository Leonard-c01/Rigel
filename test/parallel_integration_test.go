package main

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/rigel/config"
	"github.com/rigel/internal/proxy"
	"github.com/rigel/pkg/log"
)

func TestParallelProxyIntegration(t *testing.T) {
	// 初始化日志
	log.Init("info", "")

	// 启动测试服务器
	testServer := startParallelTestServer(t, "127.0.0.1:8091")
	defer testServer.Close()

	// 创建支持并行连接的动态代理配置
	cfg := &config.Config{
		Common: config.CommonConfig{
			LogLevel: "info",
			NodeID:   "parallel-test-node",
			Region:   "test-region",
		},
		Listeners: []config.ListenerConfig{
			{
				Name: "parallel-listener",
				Bind: "127.0.0.1:8090",
			},
		},
		Scheduler: config.SchedulerConfig{
			Address:           "127.0.0.1:9999",
			ConnectTimeout:    5 * time.Second,
			RequestTimeout:    3 * time.Second,
			RetryInterval:     2 * time.Second,
			MaxRetries:        2,
			HeartbeatInterval: 10 * time.Second,
		},
		Peers: []config.PeerConfig{},
	}

	// 创建并启动动态代理
	parallelProxy, err := proxy.NewUnifiedProxy(cfg)
	if err != nil {
		t.Fatalf("Failed to create parallel proxy: %v", err)
	}

	if err := parallelProxy.Start(); err != nil {
		t.Fatalf("Failed to start parallel proxy: %v", err)
	}
	defer parallelProxy.Stop()

	// 等待代理启动
	time.Sleep(200 * time.Millisecond)

	// 测试基本连接和数据转发
	testMessage := "Hello, Parallel Proxy System!"
	response := testParallelProxyConnection(t, "127.0.0.1:8090", testMessage)

	expectedResponse := fmt.Sprintf("Echo: %s", testMessage)
	if response != expectedResponse {
		t.Errorf("Expected response %q, got %q", expectedResponse, response)
	}

	// 检查统计信息
	stats := parallelProxy.GetStats()
	if stats["node_id"] != "parallel-test-node" {
		t.Errorf("Expected node ID 'parallel-test-node', got %v", stats["node_id"])
	}

	// 检查调度器连接状态
	if connected, ok := stats["scheduler_connected"].(bool); !ok || !connected {
		t.Logf("Scheduler not connected (expected in test environment)")
	}

	fmt.Printf("Parallel proxy integration test passed! Stats: %+v\n", stats)
}

func TestParallelProxyThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping throughput test in short mode")
	}

	log.Init("warn", "") // 减少日志输出

	// 启动高性能测试服务器
	testServer := startHighPerformanceServer(t, "127.0.0.1:8093")
	defer testServer.Close()

	// 创建代理配置
	cfg := &config.Config{
		Common: config.CommonConfig{
			NodeID: "throughput-test-node",
			Region: "test",
		},
		Listeners: []config.ListenerConfig{
			{
				Name: "throughput-listener",
				Bind: "127.0.0.1:8092",
			},
		},
		Scheduler: config.SchedulerConfig{
			Address:        "127.0.0.1:9999",
			ConnectTimeout: 5 * time.Second,
			RequestTimeout: 3 * time.Second,
		},
		Peers: []config.PeerConfig{},
	}

	// 创建并启动代理
	proxy, err := proxy.NewUnifiedProxy(cfg)
	if err != nil {
		t.Fatalf("Failed to create proxy: %v", err)
	}

	if err := proxy.Start(); err != nil {
		t.Fatalf("Failed to start proxy: %v", err)
	}
	defer proxy.Stop()

	time.Sleep(100 * time.Millisecond)

	// 吞吐量测试
	testDuration := 3 * time.Second
	dataSize := 1024 // 1KB per transfer
	testData := make([]byte, dataSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	var totalBytes int64
	var transferCount int64
	start := time.Now()

	for time.Since(start) < testDuration {
		response, err := testParallelProxyConnectionWithError("127.0.0.1:8092", string(testData))
		if err != nil {
			continue
		}

		totalBytes += int64(len(response))
		transferCount++
	}

	elapsed := time.Since(start)
	throughputMBps := float64(totalBytes) / elapsed.Seconds() / (1024 * 1024)

	t.Logf("Throughput test: %d transfers in %v, %.2f MB/s",
		transferCount, elapsed, throughputMBps)

	// 检查最终统计
	stats := proxy.GetStats()
	t.Logf("Final proxy stats: %+v", stats)

	if transferCount == 0 {
		t.Error("No successful transfers")
	}
}

func startParallelTestServer(t *testing.T, addr string) net.Listener {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return // Listener closed
			}
			go handleParallelTestConnection(conn)
		}
	}()

	return listener
}

func startHighPerformanceServer(t *testing.T, addr string) net.Listener {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to start high performance server: %v", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleHighPerformanceConnection(conn)
		}
	}()

	return listener
}

func handleParallelTestConnection(conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, 4096)
	n, err := conn.Read(buffer)
	if err != nil {
		return
	}

	data := buffer[:n]
	response := fmt.Sprintf("Echo: %s", string(data))
	conn.Write([]byte(response))
}

func handleHighPerformanceConnection(conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, 64*1024) // 64KB buffer
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			break
		}

		// 立即回写数据
		_, err = conn.Write(buffer[:n])
		if err != nil {
			break
		}
	}
}

func testParallelProxyConnection(t *testing.T, proxyAddr, message string) string {
	response, err := testParallelProxyConnectionWithError(proxyAddr, message)
	if err != nil {
		t.Fatalf("Failed to test proxy connection: %v", err)
	}
	return response
}

func testParallelProxyConnectionWithError(proxyAddr, message string) (string, error) {
	conn, err := net.DialTimeout("tcp", proxyAddr, 5*time.Second)
	if err != nil {
		return "", fmt.Errorf("failed to connect to proxy: %v", err)
	}
	defer conn.Close()

	// 发送消息
	_, err = conn.Write([]byte(message))
	if err != nil {
		return "", fmt.Errorf("failed to write message: %v", err)
	}

	// 读取响应
	buffer := make([]byte, 4096)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := conn.Read(buffer)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %v", err)
	}

	return string(buffer[:n]), nil
}
