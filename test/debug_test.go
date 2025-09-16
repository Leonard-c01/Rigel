package main

import (
	"fmt"

	"github.com/rigel/internal/protocol"
)

func main() {
	fmt.Println("开始调试测试...")

	// 步骤1: 创建客户端
	fmt.Println("步骤1: 创建客户端")
	client := protocol.NewBlockProtocolClient()
	if client == nil {
		fmt.Println("❌ 客户端创建失败")
		return
	}
	fmt.Println("✅ 客户端创建成功")

	// 步骤2: 编码数据
	fmt.Println("步骤2: 编码数据")
	testData := []byte("Hello")
	fmt.Printf("测试数据: %s (%d字节)\n", string(testData), len(testData))

	block, err := client.EncodeDataWithRoute(testData, "127.0.0.1:8080", 1)
	if err != nil {
		fmt.Printf("❌ 编码失败: %v\n", err)
		return
	}
	fmt.Printf("✅ 编码成功: BlockID=%d\n", block.Header.BlockID)

	// 步骤3: 序列化
	fmt.Println("步骤3: 序列化")
	serialized, err := block.Serialize()
	if err != nil {
		fmt.Printf("❌ 序列化失败: %v\n", err)
		return
	}
	fmt.Printf("✅ 序列化成功: %d字节\n", len(serialized))

	fmt.Println("调试测试完成!")
}
