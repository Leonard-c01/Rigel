package main

import (
	"fmt"

	"github.com/rigel/internal/client"
)

func main() {
	fmt.Println("最小化客户端测试")

	config := client.DefaultClientConfig()
	fmt.Printf("配置创建成功: %s\n", config.ClientID)

	fmt.Println("测试完成")
}
