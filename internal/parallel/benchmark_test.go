package parallel

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/rigel/pkg/log"
)

func init() {
	log.Init("info", "") // 减少日志输出以提高性能测试准确性
}

// BenchmarkSingleConnection 单连接基准测试
func BenchmarkSingleConnection(b *testing.B) {
	// 启动测试服务器
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("Failed to start test server: %v", err)
	}
	defer listener.Close()

	target := listener.Addr().String()

	// 启动高性能回显服务器
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleBenchmarkConnection(conn)
		}
	}()

	// 测试数据
	testData := make([]byte, 1024) // 1KB
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(testData)))

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := net.DialTimeout("tcp", target, 5*time.Second)
			if err != nil {
				b.Fatalf("Failed to connect: %v", err)
			}

			// 发送数据
			_, err = conn.Write(testData)
			if err != nil {
				conn.Close()
				b.Fatalf("Failed to write: %v", err)
			}

			// 接收数据
			buffer := make([]byte, len(testData))
			_, err = conn.Read(buffer)
			if err != nil {
				conn.Close()
				b.Fatalf("Failed to read: %v", err)
			}

			conn.Close()
		}
	})
}

// BenchmarkParallelConnection 并行连接基准测试
func BenchmarkParallelConnection(b *testing.B) {
	// 启动测试服务器
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("Failed to start test server: %v", err)
	}
	defer listener.Close()

	target := listener.Addr().String()

	// 启动高性能回显服务器
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleBenchmarkConnection(conn)
		}
	}()

	// 创建并行连接配置
	config := DefaultConnectionConfig()
	config.InitialConnections = 5
	config.MaxConnections = 10
	config.BufferSize = 64 * 1024

	// 测试数据
	testData := make([]byte, 1024) // 1KB
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(testData)))

	b.RunParallel(func(pb *testing.PB) {
		// 为每个goroutine创建一个并行连接
		parallelConn := NewTCPParallelConnection(target, config)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if err := parallelConn.Connect(ctx, target); err != nil {
			b.Fatalf("Failed to connect: %v", err)
		}
		defer parallelConn.Close()

		// 等待连接建立
		time.Sleep(50 * time.Millisecond)

		for pb.Next() {
			// 发送数据
			err := parallelConn.Send(testData)
			if err != nil {
				b.Fatalf("Failed to send: %v", err)
			}

			// 接收数据
			_, err = parallelConn.Receive()
			if err != nil {
				b.Fatalf("Failed to receive: %v", err)
			}
		}
	})
}

// BenchmarkDataSplitter 数据分发器基准测试
func BenchmarkDataSplitter(b *testing.B) {
	// 创建模拟连接
	connections := make([]*ManagedConnection, 10)
	for i := 0; i < 10; i++ {
		connections[i] = &ManagedConnection{
			ID:     fmt.Sprintf("conn%d", i),
			Status: ConnectionStatusIdle,
		}
	}

	// 测试数据
	testData := make([]byte, 64*1024) // 64KB
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	b.Run("RoundRobin", func(b *testing.B) {
		splitter := NewRoundRobinSplitter()
		b.ResetTimer()
		b.SetBytes(int64(len(testData)))

		for i := 0; i < b.N; i++ {
			_, err := splitter.Split(testData, connections)
			if err != nil {
				b.Fatalf("Failed to split: %v", err)
			}
		}
	})

	b.Run("Weighted", func(b *testing.B) {
		splitter := NewWeightedSplitter()
		// 设置权重
		for i, conn := range connections {
			splitter.SetWeight(conn.ID, float64(i+1)/10.0)
		}

		b.ResetTimer()
		b.SetBytes(int64(len(testData)))

		for i := 0; i < b.N; i++ {
			_, err := splitter.Split(testData, connections)
			if err != nil {
				b.Fatalf("Failed to split: %v", err)
			}
		}
	})

	b.Run("LeastConn", func(b *testing.B) {
		splitter := NewLeastConnSplitter()
		b.ResetTimer()
		b.SetBytes(int64(len(testData)))

		for i := 0; i < b.N; i++ {
			_, err := splitter.Split(testData, connections)
			if err != nil {
				b.Fatalf("Failed to split: %v", err)
			}
		}
	})
}

// BenchmarkDataReassembler 数据重组器基准测试
func BenchmarkDataReassembler(b *testing.B) {
	// 创建测试数据段
	segmentSize := 1024
	numSegments := 64
	segments := make([]*DataSegment, numSegments)

	for i := 0; i < numSegments; i++ {
		data := make([]byte, segmentSize)
		for j := range data {
			data[j] = byte(j % 256)
		}

		segments[i] = &DataSegment{
			SequenceID:   uint64(i + 1),
			ConnectionID: fmt.Sprintf("conn%d", i%10),
			Data:         data,
			Length:       segmentSize,
			Timestamp:    time.Now(),
			IsLast:       i == numSegments-1,
		}
	}

	b.Run("Sequential", func(b *testing.B) {
		b.ResetTimer()
		b.SetBytes(int64(segmentSize * numSegments))

		for i := 0; i < b.N; i++ {
			reassembler := NewSequentialReassembler()
			for _, segment := range segments {
				reassembler.AddSegment(segment)
			}
			reassembler.GetCompleteData()
		}
	})

	b.Run("Buffered", func(b *testing.B) {
		b.ResetTimer()
		b.SetBytes(int64(segmentSize * numSegments))

		for i := 0; i < b.N; i++ {
			reassembler := NewBufferedReassembler(1024 * 1024) // 1MB buffer
			for _, segment := range segments {
				reassembler.AddSegment(segment)
			}
			reassembler.GetCompleteData()
		}
	})
}

// handleBenchmarkConnection 处理基准测试连接
func handleBenchmarkConnection(conn net.Conn) {
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

// TestThroughputComparison 吞吐量对比测试
func TestThroughputComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping throughput test in short mode")
	}

	// 启动测试服务器
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer listener.Close()

	target := listener.Addr().String()

	// 启动高性能服务器
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleBenchmarkConnection(conn)
		}
	}()

	// 测试参数
	testDuration := 5 * time.Second
	dataSize := 64 * 1024 // 64KB per transfer
	testData := make([]byte, dataSize)

	// 单连接吞吐量测试
	t.Run("SingleConnection", func(t *testing.T) {
		var totalBytes int64
		var transferCount int64
		start := time.Now()

		for time.Since(start) < testDuration {
			conn, err := net.DialTimeout("tcp", target, 5*time.Second)
			if err != nil {
				continue
			}

			_, err = conn.Write(testData)
			if err != nil {
				conn.Close()
				continue
			}

			buffer := make([]byte, dataSize)
			n, err := conn.Read(buffer)
			if err != nil {
				conn.Close()
				continue
			}

			totalBytes += int64(n)
			transferCount++
			conn.Close()
		}

		elapsed := time.Since(start)
		throughputMBps := float64(totalBytes) / elapsed.Seconds() / (1024 * 1024)

		t.Logf("Single connection: %d transfers, %.2f MB/s", transferCount, throughputMBps)
	})

	// 并行连接吞吐量测试
	t.Run("ParallelConnection", func(t *testing.T) {
		config := DefaultConnectionConfig()
		config.InitialConnections = 5
		config.MaxConnections = 10

		parallelConn := NewTCPParallelConnection(target, config)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if err := parallelConn.Connect(ctx, target); err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		defer parallelConn.Close()

		// 等待连接建立
		time.Sleep(100 * time.Millisecond)

		var totalBytes int64
		var transferCount int64
		start := time.Now()

		for time.Since(start) < testDuration {
			err := parallelConn.Send(testData)
			if err != nil {
				continue
			}

			data, err := parallelConn.Receive()
			if err != nil {
				continue
			}

			totalBytes += int64(len(data))
			transferCount++
		}

		elapsed := time.Since(start)
		throughputMBps := float64(totalBytes) / elapsed.Seconds() / (1024 * 1024)

		t.Logf("Parallel connection: %d transfers, %.2f MB/s", transferCount, throughputMBps)

		// 获取统计信息
		stats := parallelConn.GetStats()
		t.Logf("Parallel stats: %+v", stats)
	})
}
