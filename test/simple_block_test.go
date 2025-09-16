package main

import (
	"fmt"
	"log"

	"github.com/rigel/internal/protocol"
)

func main() {
	fmt.Println("🚀 块协议单元测试")
	fmt.Println("============================================================")

	testBasicProtocol()
	testMultipleBlocks()
	testPartialData()

	fmt.Println("\n🎉 所有测试通过！")
}

func testBasicProtocol() {
	fmt.Println("\n🧪 测试基本块协议功能")

	// 创建客户端
	client := protocol.NewBlockProtocolClient()

	// 测试数据
	testData := []byte("Hello, Block Protocol!")
	targetAddress := "127.0.0.1:8080"
	priority := uint8(1)

	// 编码数据块
	block, err := client.EncodeDataWithRoute(testData, targetAddress, priority)
	if err != nil {
		log.Fatalf("Failed to encode data block: %v", err)
	}

	fmt.Printf("✅ 成功创建数据块: ID=%d, Size=%d, Target=%s\n",
		block.Header.BlockID, block.Header.BlockSize, block.Header.RouteInfo.TargetAddress)

	// 序列化数据块
	serialized, err := block.Serialize()
	if err != nil {
		log.Fatalf("Failed to serialize block: %v", err)
	}

	fmt.Printf("✅ 序列化成功: 总大小=%d字节 (头部=%d + 数据=%d)\n",
		len(serialized), 64, len(testData))

	// 创建服务端处理器
	server := protocol.NewBlockProtocolServer()

	// 处理序列化的数据
	blocks, err := server.ProcessIncomingData(serialized)
	if err != nil {
		log.Fatalf("Failed to process incoming data: %v", err)
	}

	if len(blocks) != 1 {
		log.Fatalf("Expected 1 block, got %d", len(blocks))
	}

	receivedBlock := blocks[0]
	fmt.Printf("✅ 成功解析数据块: ID=%d, Target=%s\n",
		receivedBlock.Header.BlockID, receivedBlock.Header.RouteInfo.TargetAddress)

	// 验证数据完整性
	if err := receivedBlock.ValidateData(); err != nil {
		log.Fatalf("Block validation failed: %v", err)
	}

	// 验证数据内容
	if string(receivedBlock.Data) != string(testData) {
		log.Fatalf("Data mismatch: expected %s, got %s", string(testData), string(receivedBlock.Data))
	}

	// 验证路由信息
	routeInfo := server.ExtractRouteInfo(receivedBlock)
	if routeInfo.TargetAddress != targetAddress {
		log.Fatalf("Target address mismatch: expected %s, got %s", targetAddress, routeInfo.TargetAddress)
	}

	if routeInfo.Priority != priority {
		log.Fatalf("Priority mismatch: expected %d, got %d", priority, routeInfo.Priority)
	}

	fmt.Println("✅ 基本协议测试通过！")
}

func testMultipleBlocks() {
	fmt.Println("\n🧪 测试多个数据块处理")

	client := protocol.NewBlockProtocolClient()
	server := protocol.NewBlockProtocolServer()

	// 创建多个数据块
	testCases := []struct {
		data     string
		target   string
		priority uint8
	}{
		{"Data block 1", "127.0.0.1:8001", 1},
		{"Data block 2", "127.0.0.1:8002", 2},
		{"Data block 3", "127.0.0.1:8003", 3},
	}

	var allSerialized []byte

	// 编码所有数据块
	for i, tc := range testCases {
		block, err := client.EncodeDataWithRoute([]byte(tc.data), tc.target, tc.priority)
		if err != nil {
			log.Fatalf("Failed to encode block %d: %v", i, err)
		}

		serialized, err := block.Serialize()
		if err != nil {
			log.Fatalf("Failed to serialize block %d: %v", i, err)
		}

		allSerialized = append(allSerialized, serialized...)
		fmt.Printf("✅ 编码数据块 %d: %s -> %s\n", i+1, tc.data, tc.target)
	}

	// 一次性处理所有数据
	blocks, err := server.ProcessIncomingData(allSerialized)
	if err != nil {
		log.Fatalf("Failed to process multiple blocks: %v", err)
	}

	if len(blocks) != len(testCases) {
		log.Fatalf("Expected %d blocks, got %d", len(testCases), len(blocks))
	}

	// 验证每个数据块
	for i, block := range blocks {
		expectedData := testCases[i].data
		expectedTarget := testCases[i].target
		expectedPriority := testCases[i].priority

		if string(block.Data) != expectedData {
			log.Fatalf("Block %d data mismatch: expected %s, got %s",
				i, expectedData, string(block.Data))
		}

		if block.Header.RouteInfo.TargetAddress != expectedTarget {
			log.Fatalf("Block %d target mismatch: expected %s, got %s",
				i, expectedTarget, block.Header.RouteInfo.TargetAddress)
		}

		if block.Header.RouteInfo.Priority != expectedPriority {
			log.Fatalf("Block %d priority mismatch: expected %d, got %d",
				i, expectedPriority, block.Header.RouteInfo.Priority)
		}

		fmt.Printf("✅ 验证数据块 %d: %s -> %s (优先级=%d)\n",
			i+1, string(block.Data), block.Header.RouteInfo.TargetAddress, block.Header.RouteInfo.Priority)
	}

	fmt.Println("✅ 多数据块处理测试通过！")
}

func testPartialData() {
	fmt.Println("\n🧪 测试部分数据处理")

	client := protocol.NewBlockProtocolClient()
	server := protocol.NewBlockProtocolServer()

	// 创建数据块
	testData := []byte("Test partial data processing")
	block, err := client.EncodeDataWithRoute(testData, "127.0.0.1:8080", 1)
	if err != nil {
		log.Fatalf("Failed to encode block: %v", err)
	}

	serialized, err := block.Serialize()
	if err != nil {
		log.Fatalf("Failed to serialize block: %v", err)
	}

	// 分批发送数据（模拟TCP流的特性）
	chunkSize := 20
	for i := 0; i < len(serialized); i += chunkSize {
		end := i + chunkSize
		if end > len(serialized) {
			end = len(serialized)
		}

		chunk := serialized[i:end]
		blocks, err := server.ProcessIncomingData(chunk)
		if err != nil {
			log.Fatalf("Failed to process chunk %d: %v", i/chunkSize, err)
		}

		if i+chunkSize >= len(serialized) {
			// 最后一个块应该能解析出完整数据
			if len(blocks) != 1 {
				log.Fatalf("Expected 1 block in final chunk, got %d", len(blocks))
			}
			fmt.Printf("✅ 最终块解析成功: %s\n", string(blocks[0].Data))
		} else {
			// 中间的块应该返回空（等待更多数据）
			if len(blocks) != 0 {
				log.Fatalf("Expected 0 blocks in partial chunk, got %d", len(blocks))
			}
			fmt.Printf("✅ 部分数据块 %d 处理正确\n", i/chunkSize+1)
		}
	}

	fmt.Println("✅ 部分数据处理测试通过！")
}
