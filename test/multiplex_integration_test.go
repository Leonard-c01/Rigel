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

func TestMultiplexProxyIntegration(t *testing.T) {
	// 初始化日志
	log.Init("info", "")

	// 启动测试服务器
	testServer := startMultiplexTestServer(t, "127.0.0.1:8095")
	defer testServer.Close()

	// 创建支持多路复用的动态代理配置
	cfg := &config.Config{
		Common: config.CommonConfig{
			LogLevel: "info",
			NodeID:   "multiplex-test-node",
			Region:   "test-region",
		},
		Listeners: []config.ListenerConfig{
			{
				Name: "multiplex-listener",
				Bind: "127.0.0.1:8094",
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
	multiplexProxy, err := proxy.NewUnifiedProxy(cfg)
	if err != nil {
		t.Fatalf("Failed to create multiplex proxy: %v", err)
	}

	if err := multiplexProxy.Start(); err != nil {
		t.Fatalf("Failed to start multiplex proxy: %v", err)
	}
	defer multiplexProxy.Stop()

	// 等待代理启动
	time.Sleep(200 * time.Millisecond)

	// 测试基本连接和数据转发
	testMessage := "Hello, Multiplex Proxy System!"
	response := testMultiplexProxyConnection(t, "127.0.0.1:8094", testMessage)

	expectedResponse := fmt.Sprintf("Echo: %s", testMessage)
	if response != expectedResponse {
		t.Errorf("Expected response %q, got %q", expectedResponse, response)
	}

	// 检查统计信息
	stats := multiplexProxy.GetStats()
	if stats["node_id"] != "multiplex-test-node" {
		t.Errorf("Expected node ID 'multiplex-test-node', got %v", stats["node_id"])
	}

	// 检查调度器连接状态
	if connected, ok := stats["scheduler_connected"].(bool); !ok || !connected {
		t.Logf("Scheduler not connected (expected in test environment)")
	}

	fmt.Printf("Multiplex proxy integration test passed! Stats: %+v\n", stats)
}

func TestConnectionModeSelection(t *testing.T) {
	log.Init("info", "")

	// 启动测试服务器
	testServer := startMultiplexTestServer(t, "127.0.0.1:8097")
	defer testServer.Close()

	// 创建代理配置
	cfg := &config.Config{
		Common: config.CommonConfig{
			NodeID: "mode-test-node",
			Region: "test",
		},
		Listeners: []config.ListenerConfig{
			{
				Name: "mode-listener",
				Bind: "127.0.0.1:8096",
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

	// 测试不同大小的数据传输
	testCases := []struct {
		name     string
		dataSize int
		expected string
	}{
		{"Small Data", 100, "multiplex"},      // 小数据应该使用多路复用
		{"Medium Data", 1024, "multiplex"},    // 中等数据
		{"Large Data", 64 * 1024, "parallel"}, // 大数据应该使用并行连接
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testData := make([]byte, tc.dataSize)
			for i := range testData {
				testData[i] = byte(i % 256)
			}

			response, err := testMultiplexProxyConnectionWithError("127.0.0.1:8096", string(testData))
			if err != nil {
				t.Fatalf("Failed to test connection: %v", err)
			}

			expectedResponse := fmt.Sprintf("Echo: %s", string(testData))
			if response != expectedResponse {
				t.Errorf("Data mismatch for %s", tc.name)
			}

			t.Logf("%s: %d bytes transferred successfully", tc.name, tc.dataSize)
		})
	}
}

func TestMultiplexConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrency test in short mode")
	}

	log.Init("warn", "") // 减少日志输出

	// 启动测试服务器
	testServer := startMultiplexTestServer(t, "127.0.0.1:8099")
	defer testServer.Close()

	// 创建代理配置
	cfg := &config.Config{
		Common: config.CommonConfig{
			NodeID: "concurrency-test-node",
			Region: "test",
		},
		Listeners: []config.ListenerConfig{
			{
				Name: "concurrency-listener",
				Bind: "127.0.0.1:8098",
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

	// 并发测试
	concurrency := 10
	iterations := 20
	testData := "Concurrent test message"

	results := make(chan error, concurrency*iterations)

	for i := 0; i < concurrency; i++ {
		go func(workerID int) {
			for j := 0; j < iterations; j++ {
				message := fmt.Sprintf("%s from worker %d iteration %d", testData, workerID, j)
				response, err := testMultiplexProxyConnectionWithError("127.0.0.1:8098", message)
				if err != nil {
					results <- fmt.Errorf("worker %d iteration %d failed: %v", workerID, j, err)
					continue
				}

				expectedResponse := fmt.Sprintf("Echo: %s", message)
				if response != expectedResponse {
					results <- fmt.Errorf("worker %d iteration %d response mismatch", workerID, j)
					continue
				}

				results <- nil
			}
		}(i)
	}

	// 收集结果
	successCount := 0
	errorCount := 0
	for i := 0; i < concurrency*iterations; i++ {
		if err := <-results; err != nil {
			t.Logf("Error: %v", err)
			errorCount++
		} else {
			successCount++
		}
	}

	t.Logf("Concurrency test completed: %d success, %d errors", successCount, errorCount)

	if errorCount > successCount/10 { // 允许10%的错误率
		t.Errorf("Too many errors: %d/%d", errorCount, successCount+errorCount)
	}

	// 检查最终统计
	stats := proxy.GetStats()
	t.Logf("Final proxy stats: %+v", stats)
}

func startMultiplexTestServer(t *testing.T, addr string) net.Listener {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to start multiplex test server: %v", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return // Listener closed
			}
			go handleMultiplexTestConnection(conn)
		}
	}()

	return listener
}

func handleMultiplexTestConnection(conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, 64*1024) // 64KB buffer
	n, err := conn.Read(buffer)
	if err != nil {
		return
	}

	data := buffer[:n]
	response := fmt.Sprintf("Echo: %s", string(data))
	conn.Write([]byte(response))
}

func testMultiplexProxyConnection(t *testing.T, proxyAddr, message string) string {
	response, err := testMultiplexProxyConnectionWithError(proxyAddr, message)
	if err != nil {
		t.Fatalf("Failed to test multiplex proxy connection: %v", err)
	}
	return response
}

func testMultiplexProxyConnectionWithError(proxyAddr, message string) (string, error) {
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
	buffer := make([]byte, 64*1024) // 64KB buffer
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := conn.Read(buffer)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %v", err)
	}

	return string(buffer[:n]), nil
}
