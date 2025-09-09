package main

import (
	"fmt"
	"hash/crc32"
	"tcp-proxy/internal/protocol"
)

func main() {
	fmt.Println("🔍 调试校验和问题")

	// 创建测试数据
	testData := []byte("Hello, buffered forwarding!")
	fmt.Printf("原始数据: %s\n", string(testData))
	fmt.Printf("原始数据长度: %d\n", len(testData))
	fmt.Printf("原始数据校验和: 0x%x\n", crc32.ChecksumIEEE(testData))

	// 创建数据块
	client := protocol.NewBlockProtocolClient()
	block, err := client.EncodeDataWithRoute(testData, "127.0.0.1:8081", 1)
	if err != nil {
		fmt.Printf("❌ 创建数据块失败: %v\n", err)
		return
	}

	fmt.Printf("\n📦 数据块信息:\n")
	fmt.Printf("  BlockID: %d\n", block.Header.BlockID)
	fmt.Printf("  BlockSize: %d\n", block.Header.BlockSize)
	fmt.Printf("  Header Checksum: 0x%x\n", block.Header.Checksum)
	fmt.Printf("  Data长度: %d\n", len(block.Data))
	fmt.Printf("  Data内容: %s\n", string(block.Data))
	fmt.Printf("  Data校验和: 0x%x\n", crc32.ChecksumIEEE(block.Data))

	// 验证数据块
	fmt.Printf("\n🔍 验证数据块:\n")
	if err := block.ValidateData(); err != nil {
		fmt.Printf("❌ 验证失败: %v\n", err)
	} else {
		fmt.Printf("✅ 验证成功\n")
	}

	// 序列化数据块
	serialized, err := block.Serialize()
	if err != nil {
		fmt.Printf("❌ 序列化失败: %v\n", err)
		return
	}

	fmt.Printf("\n📤 序列化信息:\n")
	fmt.Printf("  序列化长度: %d\n", len(serialized))
	fmt.Printf("  头部大小: %d\n", protocol.BlockHeaderSize)
	fmt.Printf("  数据部分长度: %d\n", len(serialized)-protocol.BlockHeaderSize)

	// 手动解析头部进行调试
	fmt.Printf("\n🔍 手动解析头部:\n")
	if len(serialized) >= protocol.BlockHeaderSize {
		header, err := protocol.DeserializeHeader(serialized[:protocol.BlockHeaderSize])
		if err != nil {
			fmt.Printf("❌ 头部解析失败: %v\n", err)
		} else {
			fmt.Printf("  解析的Checksum: 0x%x\n", header.Checksum)
			fmt.Printf("  解析的BlockSize: %d\n", header.BlockSize)
		}
	}

	// 使用服务器解析
	server := protocol.NewBlockProtocolServer()
	blocks, err := server.ProcessIncomingData(serialized)
	if err != nil {
		fmt.Printf("❌ 服务器处理失败: %v\n", err)
		return
	}

	if len(blocks) != 1 {
		fmt.Printf("❌ 期望1个数据块，得到%d个\n", len(blocks))
		return
	}

	receivedBlock := blocks[0]
	fmt.Printf("\n📥 接收到的数据块:\n")
	fmt.Printf("  BlockID: %d\n", receivedBlock.Header.BlockID)
	fmt.Printf("  BlockSize: %d\n", receivedBlock.Header.BlockSize)
	fmt.Printf("  Header Checksum: 0x%x\n", receivedBlock.Header.Checksum)
	fmt.Printf("  Data长度: %d\n", len(receivedBlock.Data))
	fmt.Printf("  Data内容: %s\n", string(receivedBlock.Data))
	fmt.Printf("  Data校验和: 0x%x\n", crc32.ChecksumIEEE(receivedBlock.Data))

	// 验证接收到的数据块
	fmt.Printf("\n🔍 验证接收到的数据块:\n")
	if err := receivedBlock.ValidateData(); err != nil {
		fmt.Printf("❌ 验证失败: %v\n", err)
	} else {
		fmt.Printf("✅ 验证成功\n")
	}

	// 比较原始数据和接收数据
	fmt.Printf("\n🔍 数据比较:\n")
	if string(testData) == string(receivedBlock.Data) {
		fmt.Printf("✅ 数据内容一致\n")
	} else {
		fmt.Printf("❌ 数据内容不一致\n")
		fmt.Printf("  原始: %s\n", string(testData))
		fmt.Printf("  接收: %s\n", string(receivedBlock.Data))
	}

	if len(testData) == len(receivedBlock.Data) {
		fmt.Printf("✅ 数据长度一致\n")
	} else {
		fmt.Printf("❌ 数据长度不一致: %d vs %d\n", len(testData), len(receivedBlock.Data))
	}
}
