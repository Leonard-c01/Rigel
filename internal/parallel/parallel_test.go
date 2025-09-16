package parallel

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

func TestTCPConnectionPool(t *testing.T) {
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

	// 创建连接池
	config := DefaultConnectionConfig()
	config.InitialConnections = 2
	config.MaxConnections = 5

	pool := NewTCPConnectionPool(target, config)

	// 启动连接池
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Failed to start connection pool: %v", err)
	}
	defer pool.Stop()

	// 等待连接建立
	time.Sleep(100 * time.Millisecond)

	// 测试获取连接
	conn1, err := pool.GetConnection()
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}

	if conn1 == nil {
		t.Fatal("Got nil connection")
	}

	// 测试连接功能
	testData := "Hello, Pool!"
	_, err = conn1.Conn.Write([]byte(testData))
	if err != nil {
		t.Fatalf("Failed to write to connection: %v", err)
	}

	buffer := make([]byte, 1024)
	n, err := conn1.Conn.Read(buffer)
	if err != nil {
		t.Fatalf("Failed to read from connection: %v", err)
	}

	response := string(buffer[:n])
	if response != testData {
		t.Errorf("Expected %q, got %q", testData, response)
	}

	// 归还连接
	pool.ReturnConnection(conn1)

	// 检查统计信息
	stats := pool.GetStats()
	if stats.TotalConnections < 2 {
		t.Errorf("Expected at least 2 connections, got %d", stats.TotalConnections)
	}

	t.Logf("Pool stats: %+v", stats)
}

func TestDataSplitter(t *testing.T) {
	// 创建模拟连接
	connections := []*ManagedConnection{
		{
			ID:     "conn1",
			Status: ConnectionStatusIdle,
		},
		{
			ID:     "conn2",
			Status: ConnectionStatusIdle,
		},
		{
			ID:     "conn3",
			Status: ConnectionStatusIdle,
		},
	}

	testData := []byte("Hello, this is a test message for data splitting!")

	// 测试轮询分发器
	t.Run("RoundRobin", func(t *testing.T) {
		splitter := NewRoundRobinSplitter()
		segments, err := splitter.Split(testData, connections)
		if err != nil {
			t.Fatalf("Failed to split data: %v", err)
		}

		if len(segments) != len(connections) {
			t.Errorf("Expected %d segments, got %d", len(connections), len(segments))
		}

		// 验证所有数据都被分发
		totalLength := 0
		for _, segment := range segments {
			totalLength += segment.Length
		}

		if totalLength != len(testData) {
			t.Errorf("Expected total length %d, got %d", len(testData), totalLength)
		}

		t.Logf("Split %d bytes into %d segments", len(testData), len(segments))
	})

	// 测试权重分发器
	t.Run("Weighted", func(t *testing.T) {
		splitter := NewWeightedSplitter()

		// 设置权重
		splitter.SetWeight("conn1", 0.5)
		splitter.SetWeight("conn2", 0.3)
		splitter.SetWeight("conn3", 0.2)

		segments, err := splitter.Split(testData, connections)
		if err != nil {
			t.Fatalf("Failed to split data: %v", err)
		}

		if len(segments) == 0 {
			t.Error("Expected at least one segment")
		}

		t.Logf("Weighted split created %d segments", len(segments))
	})

	// 测试最少连接分发器
	t.Run("LeastConn", func(t *testing.T) {
		splitter := NewLeastConnSplitter()
		segments, err := splitter.Split(testData, connections)
		if err != nil {
			t.Fatalf("Failed to split data: %v", err)
		}

		// 最少连接策略应该只选择一个连接
		if len(segments) != 1 {
			t.Errorf("Expected 1 segment, got %d", len(segments))
		}

		if segments[0].Length != len(testData) {
			t.Errorf("Expected segment length %d, got %d", len(testData), segments[0].Length)
		}
	})
}

func TestDataReassembler(t *testing.T) {
	// 测试顺序重组器
	t.Run("Sequential", func(t *testing.T) {
		reassembler := NewSequentialReassembler()

		// 创建测试数据段
		testData := "Hello, World!"
		segments := []*DataSegment{
			{
				SequenceID:   1,
				ConnectionID: "conn1",
				Data:         []byte("Hello, "),
				Length:       7,
				Timestamp:    time.Now(),
				IsLast:       false,
			},
			{
				SequenceID:   2,
				ConnectionID: "conn2",
				Data:         []byte("World!"),
				Length:       6,
				Timestamp:    time.Now(),
				IsLast:       true,
			},
		}

		// 添加数据段
		for _, segment := range segments {
			err := reassembler.AddSegment(segment)
			if err != nil {
				t.Fatalf("Failed to add segment: %v", err)
			}
		}

		// 获取重组后的数据
		data, complete := reassembler.GetCompleteData()
		if !complete {
			t.Error("Expected complete data")
		}

		result := string(data)
		if result != testData {
			t.Errorf("Expected %q, got %q", testData, result)
		}

		// 检查统计信息
		stats := reassembler.GetStats()
		if stats.CompletedMessages != 1 {
			t.Errorf("Expected 1 completed message, got %d", stats.CompletedMessages)
		}

		t.Logf("Reassembler stats: %+v", stats)
	})

	// 测试缓冲重组器
	t.Run("Buffered", func(t *testing.T) {
		reassembler := NewBufferedReassembler(1024)

		// 创建乱序数据段
		segments := []*DataSegment{
			{
				SequenceID:   2,
				ConnectionID: "conn2",
				Data:         []byte("World!"),
				Length:       6,
				Timestamp:    time.Now(),
				IsLast:       true,
			},
			{
				SequenceID:   1,
				ConnectionID: "conn1",
				Data:         []byte("Hello, "),
				Length:       7,
				Timestamp:    time.Now(),
				IsLast:       false,
			},
		}

		// 添加数据段（乱序）
		for _, segment := range segments {
			err := reassembler.AddSegment(segment)
			if err != nil {
				t.Fatalf("Failed to add segment: %v", err)
			}
		}

		// 检查是否完成
		if !reassembler.IsCompleted() {
			t.Error("Expected reassembly to be completed")
		}

		// 获取重组后的数据
		data, complete := reassembler.GetCompleteData()
		if !complete {
			t.Error("Expected complete data")
		}

		result := string(data)
		expected := "Hello, World!"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})
}

func TestTCPParallelConnection(t *testing.T) {
	// 启动测试服务器
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer listener.Close()

	target := listener.Addr().String()

	// 启动回显服务器
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buffer := make([]byte, 1024)
				for {
					n, err := c.Read(buffer)
					if err != nil {
						break
					}
					c.Write(buffer[:n])
				}
			}(conn)
		}
	}()

	// 创建并行连接管理器
	config := DefaultConnectionConfig()
	config.InitialConnections = 3
	config.MaxConnections = 5

	parallelConn := NewTCPParallelConnection(target, config)

	// 建立连接
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := parallelConn.Connect(ctx, target); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer parallelConn.Close()

	// 等待连接建立
	time.Sleep(200 * time.Millisecond)

	// 测试发送数据
	testData := []byte("Hello, Parallel Connection!")
	err = parallelConn.Send(testData)
	if err != nil {
		t.Fatalf("Failed to send data: %v", err)
	}

	// 测试接收数据
	receivedData, err := parallelConn.Receive()
	if err != nil {
		t.Fatalf("Failed to receive data: %v", err)
	}

	if string(receivedData) != string(testData) {
		t.Errorf("Expected %q, got %q", string(testData), string(receivedData))
	}

	// 检查统计信息
	stats := parallelConn.GetStats()
	if stats.ConnectionCount < 3 {
		t.Errorf("Expected at least 3 connections, got %d", stats.ConnectionCount)
	}

	if stats.TotalBytesSent == 0 {
		t.Error("Expected bytes sent > 0")
	}

	t.Logf("Parallel connection stats: %+v", stats)
}
