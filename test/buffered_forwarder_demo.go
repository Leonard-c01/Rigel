package main

import (
	"fmt"
	"net"
	"time"
	"tcp-proxy/internal/dataplane"
	"tcp-proxy/internal/protocol"
	"tcp-proxy/pkg/log"
)

func main() {
	fmt.Println("🧪 测试改进后的缓冲区转发机制")

	// 初始化日志
	log.Init("debug", "")

	// 创建数据面转发器
	forwarder := dataplane.NewDataPlaneForwarder("127.0.0.1:8080", ":8080")

	// 启动转发器
	go func() {
		if err := forwarder.Start(); err != nil {
			fmt.Printf("❌ 启动转发器失败: %v\n", err)
			return
		}
	}()

	// 等待转发器启动
	time.Sleep(100 * time.Millisecond)

	// 创建测试数据块
	client := protocol.NewBlockProtocolClient()
	testData := []byte("Hello, buffered forwarding!")
	
	// 创建数据块，路径指向下一跳
	block, err := client.EncodeDataWithRoute(testData, "127.0.0.1:8081->127.0.0.1:8082", 1)
	if err != nil {
		fmt.Printf("❌ 创建数据块失败: %v\n", err)
		return
	}

	// 序列化数据块
	serialized, err := block.Serialize()
	if err != nil {
		fmt.Printf("❌ 序列化失败: %v\n", err)
		return
	}

	fmt.Printf("✅ 创建测试数据块: ID=%d, 大小=%d字节\n", block.Header.BlockID, len(serialized))

	// 测试1: 完整数据块发送
	fmt.Println("\n📦 测试1: 发送完整数据块")
	if err := testCompleteBlock(serialized); err != nil {
		fmt.Printf("❌ 完整数据块测试失败: %v\n", err)
	} else {
		fmt.Println("✅ 完整数据块测试通过")
	}

	// 测试2: 分片数据发送（模拟TCP流分片）
	fmt.Println("\n📦 测试2: 发送分片数据")
	if err := testFragmentedBlock(serialized); err != nil {
		fmt.Printf("❌ 分片数据测试失败: %v\n", err)
	} else {
		fmt.Println("✅ 分片数据测试通过")
	}

	// 测试3: 多个数据块连续发送
	fmt.Println("\n📦 测试3: 连续发送多个数据块")
	if err := testMultipleBlocks(client); err != nil {
		fmt.Printf("❌ 多数据块测试失败: %v\n", err)
	} else {
		fmt.Println("✅ 多数据块测试通过")
	}

	fmt.Println("\n🎉 所有缓冲区测试完成！")
}

// testCompleteBlock 测试完整数据块发送
func testCompleteBlock(data []byte) error {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:8080", 5*time.Second)
	if err != nil {
		return fmt.Errorf("连接失败: %v", err)
	}
	defer conn.Close()

	// 一次性发送完整数据
	_, err = conn.Write(data)
	if err != nil {
		return fmt.Errorf("发送失败: %v", err)
	}

	fmt.Printf("  ✅ 发送完整数据块: %d字节\n", len(data))
	time.Sleep(100 * time.Millisecond) // 等待处理
	return nil
}

// testFragmentedBlock 测试分片数据发送
func testFragmentedBlock(data []byte) error {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:8080", 5*time.Second)
	if err != nil {
		return fmt.Errorf("连接失败: %v", err)
	}
	defer conn.Close()

	// 分片发送数据（每次发送20字节）
	chunkSize := 20
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}

		chunk := data[i:end]
		_, err = conn.Write(chunk)
		if err != nil {
			return fmt.Errorf("发送分片失败: %v", err)
		}

		fmt.Printf("  📤 发送分片 %d: %d字节\n", i/chunkSize+1, len(chunk))
		time.Sleep(10 * time.Millisecond) // 模拟网络延迟
	}

	time.Sleep(100 * time.Millisecond) // 等待处理完成
	return nil
}

// testMultipleBlocks 测试多个数据块发送
func testMultipleBlocks(client *protocol.BlockProtocolClient) error {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:8080", 5*time.Second)
	if err != nil {
		return fmt.Errorf("连接失败: %v", err)
	}
	defer conn.Close()

	// 创建多个数据块
	testMessages := []string{
		"First message",
		"Second message",
		"Third message",
	}

	var allData []byte
	for i, msg := range testMessages {
		block, err := client.EncodeDataWithRoute([]byte(msg), 
			fmt.Sprintf("127.0.0.1:808%d", i+1), uint8(i+1))
		if err != nil {
			return fmt.Errorf("创建数据块%d失败: %v", i+1, err)
		}

		serialized, err := block.Serialize()
		if err != nil {
			return fmt.Errorf("序列化数据块%d失败: %v", i+1, err)
		}

		allData = append(allData, serialized...)
		fmt.Printf("  📦 准备数据块%d: %s\n", i+1, msg)
	}

	// 一次性发送所有数据块
	_, err = conn.Write(allData)
	if err != nil {
		return fmt.Errorf("发送多数据块失败: %v", err)
	}

	fmt.Printf("  ✅ 发送%d个数据块，总计%d字节\n", len(testMessages), len(allData))
	time.Sleep(200 * time.Millisecond) // 等待处理
	return nil
}
