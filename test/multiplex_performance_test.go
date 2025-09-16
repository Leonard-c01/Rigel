package main

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/rigel/config"
	"github.com/rigel/internal/proxy"
	"github.com/rigel/pkg/log"
)

func TestConnectionModePerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	log.Init("warn", "") // 减少日志输出

	// 测试参数
	testDuration := 3 * time.Second
	dataSize := 1024 // 1KB per transfer
	testData := make([]byte, dataSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// 测试结果
	results := make(map[string]*PerformanceResult)

	// 1. 测试单连接性能
	t.Run("SingleConnection", func(t *testing.T) {
		result := testSingleConnectionPerformance(t, testDuration, testData)
		results["single"] = result
		t.Logf("Single connection: %d transfers, %.2f MB/s, %.2f ms avg latency",
			result.TransferCount, result.ThroughputMBps, result.AvgLatencyMs)
	})

	// 2. 测试并行连接性能
	t.Run("ParallelConnection", func(t *testing.T) {
		result := testParallelConnectionPerformance(t, testDuration, testData)
		results["parallel"] = result
		t.Logf("Parallel connection: %d transfers, %.2f MB/s, %.2f ms avg latency",
			result.TransferCount, result.ThroughputMBps, result.AvgLatencyMs)
	})

	// 3. 测试多路复用性能
	t.Run("MultiplexConnection", func(t *testing.T) {
		result := testMultiplexConnectionPerformance(t, testDuration, testData)
		results["multiplex"] = result
		t.Logf("Multiplex connection: %d transfers, %.2f MB/s, %.2f ms avg latency",
			result.TransferCount, result.ThroughputMBps, result.AvgLatencyMs)
	})

	// 4. 测试混合连接性能
	t.Run("HybridConnection", func(t *testing.T) {
		result := testHybridConnectionPerformance(t, testDuration, testData)
		results["hybrid"] = result
		t.Logf("Hybrid connection: %d transfers, %.2f MB/s, %.2f ms avg latency",
			result.TransferCount, result.ThroughputMBps, result.AvgLatencyMs)
	})

	// 性能对比分析
	t.Run("PerformanceComparison", func(t *testing.T) {
		analyzePerformance(t, results)
	})
}

func TestConcurrentConnectionsPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent performance test in short mode")
	}

	log.Init("warn", "")

	// 测试不同并发级别的性能
	concurrencyLevels := []int{1, 5, 10, 20, 50}
	testDuration := 2 * time.Second
	dataSize := 512
	testData := make([]byte, dataSize)

	for _, concurrency := range concurrencyLevels {
		t.Run(fmt.Sprintf("Concurrency_%d", concurrency), func(t *testing.T) {
			result := testConcurrentPerformance(t, concurrency, testDuration, testData)
			t.Logf("Concurrency %d: %d total transfers, %.2f MB/s total throughput",
				concurrency, result.TransferCount, result.ThroughputMBps)
		})
	}
}

// PerformanceResult 性能测试结果
type PerformanceResult struct {
	TransferCount  int64
	TotalBytes     int64
	Duration       time.Duration
	ThroughputMBps float64
	AvgLatencyMs   float64
	ErrorCount     int64
}

func testSingleConnectionPerformance(t *testing.T, duration time.Duration, testData []byte) *PerformanceResult {
	// 启动测试服务器
	server := startPerformanceTestServer(t, "127.0.0.1:0")
	defer server.Close()

	serverAddr := server.Addr().String()

	var transferCount, totalBytes, errorCount int64
	var totalLatency time.Duration
	start := time.Now()

	for time.Since(start) < duration {
		latencyStart := time.Now()

		conn, err := net.DialTimeout("tcp", serverAddr, 5*time.Second)
		if err != nil {
			errorCount++
			continue
		}

		_, err = conn.Write(testData)
		if err != nil {
			conn.Close()
			errorCount++
			continue
		}

		buffer := make([]byte, len(testData))
		_, err = conn.Read(buffer)
		conn.Close()

		latency := time.Since(latencyStart)
		totalLatency += latency

		if err != nil {
			errorCount++
			continue
		}

		transferCount++
		totalBytes += int64(len(testData))
	}

	elapsed := time.Since(start)
	throughputMBps := float64(totalBytes) / elapsed.Seconds() / (1024 * 1024)
	avgLatencyMs := float64(totalLatency.Nanoseconds()) / float64(transferCount) / 1e6

	return &PerformanceResult{
		TransferCount:  transferCount,
		TotalBytes:     totalBytes,
		Duration:       elapsed,
		ThroughputMBps: throughputMBps,
		AvgLatencyMs:   avgLatencyMs,
		ErrorCount:     errorCount,
	}
}

func testParallelConnectionPerformance(t *testing.T, duration time.Duration, testData []byte) *PerformanceResult {
	// 启动测试服务器
	server := startPerformanceTestServer(t, "127.0.0.1:0")
	defer server.Close()

	// 创建支持并行连接的代理
	cfg := createProxyConfig("127.0.0.1:0", server.Addr().String())
	proxy, err := proxy.NewUnifiedProxy(cfg)
	if err != nil {
		t.Fatalf("Failed to create proxy: %v", err)
	}

	if err := proxy.Start(); err != nil {
		t.Fatalf("Failed to start proxy: %v", err)
	}
	defer proxy.Stop()

	time.Sleep(100 * time.Millisecond)

	// 获取代理监听地址
	proxyAddr := "127.0.0.1:8090" // 从配置中获取

	return performTransferTest(t, proxyAddr, duration, testData)
}

func testMultiplexConnectionPerformance(t *testing.T, duration time.Duration, testData []byte) *PerformanceResult {
	// 启动测试服务器
	server := startPerformanceTestServer(t, "127.0.0.1:0")
	defer server.Close()

	// 创建支持多路复用的代理
	cfg := createProxyConfig("127.0.0.1:0", server.Addr().String())
	proxy, err := proxy.NewUnifiedProxy(cfg)
	if err != nil {
		t.Fatalf("Failed to create proxy: %v", err)
	}

	if err := proxy.Start(); err != nil {
		t.Fatalf("Failed to start proxy: %v", err)
	}
	defer proxy.Stop()

	time.Sleep(100 * time.Millisecond)

	proxyAddr := "127.0.0.1:8090"
	return performTransferTest(t, proxyAddr, duration, testData)
}

func testHybridConnectionPerformance(t *testing.T, duration time.Duration, testData []byte) *PerformanceResult {
	// 启动测试服务器
	server := startPerformanceTestServer(t, "127.0.0.1:0")
	defer server.Close()

	// 创建支持混合连接的代理
	cfg := createProxyConfig("127.0.0.1:0", server.Addr().String())
	proxy, err := proxy.NewUnifiedProxy(cfg)
	if err != nil {
		t.Fatalf("Failed to create proxy: %v", err)
	}

	if err := proxy.Start(); err != nil {
		t.Fatalf("Failed to start proxy: %v", err)
	}
	defer proxy.Stop()

	time.Sleep(100 * time.Millisecond)

	proxyAddr := "127.0.0.1:8090"
	return performTransferTest(t, proxyAddr, duration, testData)
}

func testConcurrentPerformance(t *testing.T, concurrency int, duration time.Duration, testData []byte) *PerformanceResult {
	// 启动测试服务器
	server := startPerformanceTestServer(t, "127.0.0.1:0")
	defer server.Close()

	serverAddr := server.Addr().String()

	var wg sync.WaitGroup
	var totalTransfers, totalBytes, totalErrors int64
	var totalLatency time.Duration
	var mu sync.Mutex

	start := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var transfers, bytes, errors int64
			var latency time.Duration

			for time.Since(start) < duration {
				latencyStart := time.Now()

				conn, err := net.DialTimeout("tcp", serverAddr, 5*time.Second)
				if err != nil {
					errors++
					continue
				}

				_, err = conn.Write(testData)
				if err != nil {
					conn.Close()
					errors++
					continue
				}

				buffer := make([]byte, len(testData))
				_, err = conn.Read(buffer)
				conn.Close()

				latency += time.Since(latencyStart)

				if err != nil {
					errors++
					continue
				}

				transfers++
				bytes += int64(len(testData))
			}

			mu.Lock()
			totalTransfers += transfers
			totalBytes += bytes
			totalErrors += errors
			totalLatency += latency
			mu.Unlock()
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	throughputMBps := float64(totalBytes) / elapsed.Seconds() / (1024 * 1024)
	avgLatencyMs := float64(totalLatency.Nanoseconds()) / float64(totalTransfers) / 1e6

	return &PerformanceResult{
		TransferCount:  totalTransfers,
		TotalBytes:     totalBytes,
		Duration:       elapsed,
		ThroughputMBps: throughputMBps,
		AvgLatencyMs:   avgLatencyMs,
		ErrorCount:     totalErrors,
	}
}

func performTransferTest(t *testing.T, addr string, duration time.Duration, testData []byte) *PerformanceResult {
	var transferCount, totalBytes, errorCount int64
	var totalLatency time.Duration
	start := time.Now()

	for time.Since(start) < duration {
		latencyStart := time.Now()

		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			errorCount++
			continue
		}

		_, err = conn.Write(testData)
		if err != nil {
			conn.Close()
			errorCount++
			continue
		}

		buffer := make([]byte, len(testData))
		_, err = conn.Read(buffer)
		conn.Close()

		latency := time.Since(latencyStart)
		totalLatency += latency

		if err != nil {
			errorCount++
			continue
		}

		transferCount++
		totalBytes += int64(len(testData))
	}

	elapsed := time.Since(start)
	throughputMBps := float64(totalBytes) / elapsed.Seconds() / (1024 * 1024)
	avgLatencyMs := float64(totalLatency.Nanoseconds()) / float64(transferCount) / 1e6

	return &PerformanceResult{
		TransferCount:  transferCount,
		TotalBytes:     totalBytes,
		Duration:       elapsed,
		ThroughputMBps: throughputMBps,
		AvgLatencyMs:   avgLatencyMs,
		ErrorCount:     errorCount,
	}
}

func startPerformanceTestServer(t *testing.T, addr string) net.Listener {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to start performance test server: %v", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handlePerformanceConnection(conn)
		}
	}()

	return listener
}

func handlePerformanceConnection(conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, 64*1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			break
		}
		_, err = conn.Write(buffer[:n])
		if err != nil {
			break
		}
	}
}

func createProxyConfig(proxyAddr, targetAddr string) *config.Config {
	return &config.Config{
		Common: config.CommonConfig{
			NodeID: "performance-test-node",
			Region: "test",
		},
		Listeners: []config.ListenerConfig{
			{
				Name: "performance-listener",
				Bind: proxyAddr,
			},
		},
		Scheduler: config.SchedulerConfig{
			Address:        "127.0.0.1:9999",
			ConnectTimeout: 5 * time.Second,
			RequestTimeout: 3 * time.Second,
		},
		Peers: []config.PeerConfig{},
	}
}

func analyzePerformance(t *testing.T, results map[string]*PerformanceResult) {
	t.Log("\n=== Performance Analysis ===")

	baseline := results["single"]
	if baseline == nil {
		t.Log("No baseline (single connection) result available")
		return
	}

	t.Logf("Baseline (Single): %.2f MB/s, %.2f ms latency",
		baseline.ThroughputMBps, baseline.AvgLatencyMs)

	for mode, result := range results {
		if mode == "single" {
			continue
		}

		throughputImprovement := (result.ThroughputMBps / baseline.ThroughputMBps) * 100
		latencyChange := ((result.AvgLatencyMs - baseline.AvgLatencyMs) / baseline.AvgLatencyMs) * 100

		t.Logf("%s: %.2f MB/s (%.1f%% of baseline), %.2f ms latency (%.1f%% change)",
			mode, result.ThroughputMBps, throughputImprovement,
			result.AvgLatencyMs, latencyChange)
	}
}
