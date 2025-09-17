package test

import (
	"testing"

	"github.com/rigel/internal/client"
	"github.com/rigel/pkg/log"
)

func TestClientBasicFunctionality(t *testing.T) {
	// 初始化日志
	if err := log.Init("info", ""); err != nil {
		t.Fatalf("Failed to initialize logger: %v", err)
	}

	// 测试1: 创建客户端
	config := client.DefaultClientConfig()
	config.ClientID = "test_client"

	rigelClient, err := client.NewRigelClient(config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer rigelClient.Close()

	t.Logf("Client created successfully: %s", config.ClientID)

	// 测试2: 获取统计信息
	stats := rigelClient.GetStats()
	if stats.ClientID != config.ClientID {
		t.Errorf("Expected ClientID %s, got %s", config.ClientID, stats.ClientID)
	}

	t.Logf("Client test completed successfully")
}
