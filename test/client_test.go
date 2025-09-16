package main

import (
	"fmt"

	"github.com/rigel/internal/client"
	"github.com/rigel/pkg/log"
)

func main() {
	fmt.Println("🧪 Client模块简单测试")

	// 初始化日志
	log.Init("info", "")

	// 测试1: 创建客户端
	fmt.Println("1️⃣ 创建客户端")
	config := client.DefaultClientConfig()
	config.ClientID = "test_client"

	rigelClient, err := client.NewRigelClient(config)
	if err != nil {
		fmt.Printf("❌ 创建客户端失败: %v\n", err)
		return
	}
	defer rigelClient.Close()

	fmt.Printf("✅ 客户端创建成功: %s\n", config.ClientID)

	// 测试2: 获取统计信息
	fmt.Println("\n2️⃣ 获取统计信息")
	stats := rigelClient.GetStats()
	fmt.Printf("✅ 统计信息获取成功: ClientID=%s\n", stats.ClientID)

	fmt.Println("\n🎉 简单测试完成！")
}
