package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/rigel/internal/controlplane"
)

func main() {
	fmt.Println("=== Rigel 弹性伸缩 API 演示 ===")

	// 启动弹性控制器和API服务器
	controller, api := setupElasticAPI()
	defer controller.Stop()

	// 启动API服务器
	go func() {
		fmt.Println("启动API服务器在端口 :8080")
		if err := api.Start(); err != nil && err != http.ErrServerClosed {
			log.Printf("API服务器错误: %v", err)
		}
	}()

	// 等待服务器启动
	time.Sleep(2 * time.Second)

	// 演示API功能
	demonstrateHealthCheck()
	demonstrateNodeOperations()
	demonstrateDecisionAPI()
	demonstrateStatisticsAPI()
	demonstrateCloudAPI()

	// 关闭API服务器
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	api.Stop(ctx)

	fmt.Println("✓ API演示完成")
}

func setupElasticAPI() (*controlplane.ElasticController, *controlplane.ElasticAPI) {
	// 创建弹性控制器
	config := controlplane.DefaultElasticControllerConfig()
	config.CloudProviders["mock"] = &controlplane.CloudProviderConfig{
		Provider: "mock",
		Region:   "test-region",
	}

	controller := controlplane.NewElasticController(config)
	if err := controller.Start(); err != nil {
		log.Fatalf("Failed to start controller: %v", err)
	}

	// 创建API服务器
	api := controlplane.NewElasticAPI(controller, ":8080")

	return controller, api
}

func demonstrateHealthCheck() {
	fmt.Println("\n=== 1. 健康检查 API ===")

	resp, err := http.Get("http://localhost:8080/health")
	if err != nil {
		log.Printf("健康检查失败: %v", err)
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("健康检查响应 (状态码: %d):\n%s\n", resp.StatusCode, string(body))
}

func demonstrateNodeOperations() {
	fmt.Println("\n=== 2. 节点操作 API ===")

	// 2.1 获取所有节点
	fmt.Println("2.1 获取所有节点:")
	resp, err := http.Get("http://localhost:8080/api/v1/elastic/nodes")
	if err != nil {
		log.Printf("获取节点失败: %v", err)
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("所有节点 (状态码: %d):\n%s\n", resp.StatusCode, string(body))

	// 2.2 更新节点成本
	fmt.Println("\n2.2 更新节点成本:")
	costData := map[string]interface{}{
		"fixed_cost":    150.0,
		"variable_cost": 2.5,
	}

	jsonData, _ := json.Marshal(costData)
	req, err := http.NewRequest("PUT", "http://localhost:8080/api/v1/elastic/nodes/demo-node/costs", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("创建请求失败: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	response, err := client.Do(req)
	if err != nil {
		log.Printf("更新成本失败: %v", err)
		return
	}
	defer response.Body.Close()

	body, _ = io.ReadAll(response.Body)
	fmt.Printf("更新成本响应 (状态码: %d):\n%s\n", response.StatusCode, string(body))

	// 2.3 获取特定节点
	fmt.Println("\n2.3 获取特定节点:")
	resp, err = http.Get("http://localhost:8080/api/v1/elastic/nodes/demo-node")
	if err != nil {
		log.Printf("获取节点失败: %v", err)
		return
	}
	defer resp.Body.Close()

	body, _ = io.ReadAll(resp.Body)
	fmt.Printf("节点详情 (状态码: %d):\n%s\n", resp.StatusCode, string(body))
}

func demonstrateDecisionAPI() {
	fmt.Println("\n=== 3. 决策 API ===")

	// 3.1 执行伸缩决策
	fmt.Println("3.1 执行伸缩决策:")
	decisionData := map[string]interface{}{
		"virtual_queue": 3.5,
	}

	jsonData, _ := json.Marshal(decisionData)
	resp, err := http.Post("http://localhost:8080/api/v1/elastic/nodes/decision-demo/decision",
		"application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("执行决策失败: %v", err)
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("决策响应 (状态码: %d):\n%s\n", resp.StatusCode, string(body))

	// 3.2 模拟决策
	fmt.Println("\n3.2 模拟决策:")
	simulateData := map[string]interface{}{
		"virtual_queue": 2.8,
	}

	jsonData, _ = json.Marshal(simulateData)
	resp, err = http.Post("http://localhost:8080/api/v1/elastic/nodes/simulate-demo/simulate",
		"application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("模拟决策失败: %v", err)
		return
	}
	defer resp.Body.Close()

	body, _ = io.ReadAll(resp.Body)
	fmt.Printf("模拟决策响应 (状态码: %d):\n%s\n", resp.StatusCode, string(body))

	// 3.3 获取决策历史
	fmt.Println("\n3.3 获取决策历史:")
	resp, err = http.Get("http://localhost:8080/api/v1/elastic/nodes/decision-demo/history/decisions?limit=5")
	if err != nil {
		log.Printf("获取决策历史失败: %v", err)
		return
	}
	defer resp.Body.Close()

	body, _ = io.ReadAll(resp.Body)
	fmt.Printf("决策历史 (状态码: %d):\n%s\n", resp.StatusCode, string(body))
}

func demonstrateStatisticsAPI() {
	fmt.Println("\n=== 4. 统计信息 API ===")

	// 4.1 获取系统统计
	fmt.Println("4.1 系统统计:")
	resp, err := http.Get("http://localhost:8080/api/v1/elastic/stats")
	if err != nil {
		log.Printf("获取统计失败: %v", err)
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("系统统计 (状态码: %d):\n%s\n", resp.StatusCode, string(body))

	// 4.2 获取状态统计
	fmt.Println("\n4.2 状态统计:")
	resp, err = http.Get("http://localhost:8080/api/v1/elastic/state-stats")
	if err != nil {
		log.Printf("获取状态统计失败: %v", err)
		return
	}
	defer resp.Body.Close()

	body, _ = io.ReadAll(resp.Body)
	fmt.Printf("状态统计 (状态码: %d):\n%s\n", resp.StatusCode, string(body))

	// 4.3 获取活跃节点
	fmt.Println("\n4.3 活跃节点:")
	resp, err = http.Get("http://localhost:8080/api/v1/elastic/nodes?filter=active")
	if err != nil {
		log.Printf("获取活跃节点失败: %v", err)
		return
	}
	defer resp.Body.Close()

	body, _ = io.ReadAll(resp.Body)
	fmt.Printf("活跃节点 (状态码: %d):\n%s\n", resp.StatusCode, string(body))

	// 4.4 获取可伸缩节点
	fmt.Println("\n4.4 可伸缩节点:")
	resp, err = http.Get("http://localhost:8080/api/v1/elastic/nodes?filter=scalable")
	if err != nil {
		log.Printf("获取可伸缩节点失败: %v", err)
		return
	}
	defer resp.Body.Close()

	body, _ = io.ReadAll(resp.Body)
	fmt.Printf("可伸缩节点 (状态码: %d):\n%s\n", resp.StatusCode, string(body))
}

func demonstrateCloudAPI() {
	fmt.Println("\n=== 5. 云实例管理 API ===")

	// 5.1 获取云实例
	fmt.Println("5.1 获取所有云实例:")
	resp, err := http.Get("http://localhost:8080/api/v1/elastic/cloud/instances")
	if err != nil {
		log.Printf("获取云实例失败: %v", err)
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("云实例列表 (状态码: %d):\n%s\n", resp.StatusCode, string(body))

	// 5.2 强制伸缩操作
	fmt.Println("\n5.2 强制伸缩操作:")
	forceData := map[string]interface{}{
		"target_state": "SCALING_UP",
		"reason":       "API演示强制伸缩",
	}

	jsonData, _ := json.Marshal(forceData)
	resp, err = http.Post("http://localhost:8080/api/v1/elastic/nodes/force-demo/force-scaling",
		"application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("强制伸缩失败: %v", err)
		return
	}
	defer resp.Body.Close()

	body, _ = io.ReadAll(resp.Body)
	fmt.Printf("强制伸缩响应 (状态码: %d):\n%s\n", resp.StatusCode, string(body))

	// 等待一下让操作完成
	time.Sleep(1 * time.Second)

	// 5.3 再次获取云实例查看变化
	fmt.Println("\n5.3 强制伸缩后的云实例:")
	resp, err = http.Get("http://localhost:8080/api/v1/elastic/cloud/instances")
	if err != nil {
		log.Printf("获取云实例失败: %v", err)
		return
	}
	defer resp.Body.Close()

	body, _ = io.ReadAll(resp.Body)
	fmt.Printf("更新后的云实例列表 (状态码: %d):\n%s\n", resp.StatusCode, string(body))
}
