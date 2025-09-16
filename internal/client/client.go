package client

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/rigel/internal/protocol"
	"github.com/rigel/pkg/log"
)

// RigelClient Rigel系统的客户端模块
type RigelClient struct {
	// 基础配置
	clientID   string
	proxyNodes []string // 可用的代理节点列表

	// 任务管理
	taskManager *TaskManager

	// 决策请求
	decisionMaker *DecisionMaker

	// 策略执行
	rateLimiter *RateLimiter
	pathEncoder *PathEncoder

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.RWMutex
}

// ClientConfig 客户端配置
type ClientConfig struct {
	ClientID       string        `json:"client_id"`
	ProxyNodes     []string      `json:"proxy_nodes"`      // 代理节点地址列表
	ChunkSize      int           `json:"chunk_size"`       // 数据块大小
	MaxConcurrency int           `json:"max_concurrency"`  // 最大并发数
	RequestTimeout time.Duration `json:"request_timeout"`  // 请求超时
	RetryAttempts  int           `json:"retry_attempts"`   // 重试次数
	RateLimitBurst int           `json:"rate_limit_burst"` // 速率限制突发
}

// DefaultClientConfig 默认客户端配置
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		ClientID:       fmt.Sprintf("client_%d", time.Now().UnixNano()),
		ProxyNodes:     []string{"127.0.0.1:8400", "127.0.0.1:8401", "127.0.0.1:8402"},
		ChunkSize:      64 * 1024, // 64KB
		MaxConcurrency: 10,
		RequestTimeout: 5 * time.Second,
		RetryAttempts:  3,
		RateLimitBurst: 100,
	}
}

// NewRigelClient 创建Rigel客户端
func NewRigelClient(config *ClientConfig) (*RigelClient, error) {
	if config == nil {
		config = DefaultClientConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	client := &RigelClient{
		clientID:   config.ClientID,
		proxyNodes: config.ProxyNodes,
		ctx:        ctx,
		cancel:     cancel,
	}

	// 初始化任务管理器
	taskManager, err := NewTaskManager(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create task manager: %v", err)
	}
	client.taskManager = taskManager

	// 初始化决策制定器
	decisionMaker, err := NewDecisionMaker(config.ProxyNodes, config.RequestTimeout, config.RetryAttempts)
	if err != nil {
		return nil, fmt.Errorf("failed to create decision maker: %v", err)
	}
	client.decisionMaker = decisionMaker

	// 初始化速率限制器
	rateLimiter := NewRateLimiter(config.RateLimitBurst)
	client.rateLimiter = rateLimiter

	// 初始化路径编码器
	pathEncoder := NewPathEncoder()
	client.pathEncoder = pathEncoder

	log.Infof("Rigel client created: ID=%s, ProxyNodes=%v", config.ClientID, config.ProxyNodes)

	return client, nil
}

// SendData 发送数据到目标地址
func (c *RigelClient) SendData(data []byte, targetAddress string) error {
	return c.SendDataWithPriority(data, targetAddress, 1)
}

// SendDataWithPriority 发送数据到目标地址（带优先级）
func (c *RigelClient) SendDataWithPriority(data []byte, targetAddress string, priority uint8) error {
	log.Infof("Client %s sending %d bytes to %s (priority=%d)",
		c.clientID, len(data), targetAddress, priority)

	// 1. 任务管理：拆分为数据块
	chunks, err := c.taskManager.SplitIntoChunks(data)
	if err != nil {
		return fmt.Errorf("failed to split data into chunks: %v", err)
	}

	log.Infof("Data split into %d chunks", len(chunks))

	// 2. 为每个数据块处理
	for i, chunk := range chunks {
		if err := c.processChunk(chunk, targetAddress, priority, i, len(chunks)); err != nil {
			log.Errorf("Failed to process chunk %d: %v", i, err)
			return err
		}
	}

	log.Infof("All chunks sent successfully")
	return nil
}

// processChunk 处理单个数据块
func (c *RigelClient) processChunk(chunk []byte, targetAddress string, priority uint8, chunkIndex, totalChunks int) error {
	// 2. 决策请求：向代理节点请求路径
	pathRequest := &PathRequest{
		SourceID:      c.clientID,
		TargetAddress: targetAddress,
		DataSize:      len(chunk),
		Priority:      priority,
		ChunkIndex:    chunkIndex,
		TotalChunks:   totalChunks,
	}

	pathResponse, err := c.decisionMaker.RequestPath(pathRequest)
	if err != nil {
		return fmt.Errorf("failed to request path: %v", err)
	}

	log.Debugf("Received path for chunk %d: %v (rate=%.2f KB/s)",
		chunkIndex, pathResponse.Path, pathResponse.RecommendedRate/1024)

	// 3. 策略执行：速率控制
	if err := c.rateLimiter.WaitForToken(pathResponse.RecommendedRate); err != nil {
		return fmt.Errorf("rate limiting failed: %v", err)
	}

	// 4. 策略执行：路径封装
	dataBlock, err := c.pathEncoder.EncodeWithPath(chunk, pathResponse.Path, priority)
	if err != nil {
		return fmt.Errorf("failed to encode path: %v", err)
	}

	// 5. 发送到代理网络
	if err := c.sendToProxy(dataBlock, pathResponse.Path[0]); err != nil {
		return fmt.Errorf("failed to send to proxy: %v", err)
	}

	return nil
}

// sendToProxy 发送数据块到代理节点
func (c *RigelClient) sendToProxy(dataBlock *protocol.DataBlock, firstHop string) error {
	// 连接到第一跳代理节点
	conn, err := net.DialTimeout("tcp", firstHop, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to proxy %s: %v", firstHop, err)
	}
	defer conn.Close()

	// 序列化并发送数据块
	serialized, err := dataBlock.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize data block: %v", err)
	}

	_, err = conn.Write(serialized)
	if err != nil {
		return fmt.Errorf("failed to write data: %v", err)
	}

	log.Debugf("Data block sent to proxy %s: %d bytes", firstHop, len(serialized))
	return nil
}

// SendFile 发送文件
func (c *RigelClient) SendFile(filePath, targetAddress string) error {
	// 这里可以实现文件读取和分块发送
	// 为了演示，暂时简化实现
	data := []byte(fmt.Sprintf("File content from %s", filePath))
	return c.SendData(data, targetAddress)
}

// SendStream 发送流数据
func (c *RigelClient) SendStream(dataStream <-chan []byte, targetAddress string) error {
	for data := range dataStream {
		if err := c.SendData(data, targetAddress); err != nil {
			return err
		}
	}
	return nil
}

// GetStats 获取客户端统计信息
func (c *RigelClient) GetStats() *ClientStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return &ClientStats{
		ClientID:         c.clientID,
		TaskStats:        c.taskManager.GetStats(),
		DecisionStats:    c.decisionMaker.GetStats(),
		RateLimiterStats: c.rateLimiter.GetStats(),
	}
}

// Close 关闭客户端
func (c *RigelClient) Close() error {
	log.Infof("Closing Rigel client %s", c.clientID)

	c.cancel()

	if c.taskManager != nil {
		c.taskManager.Close()
	}

	if c.decisionMaker != nil {
		c.decisionMaker.Close()
	}

	if c.rateLimiter != nil {
		c.rateLimiter.Close()
	}

	log.Infof("Rigel client %s closed", c.clientID)
	return nil
}

// ClientStats 客户端统计信息
type ClientStats struct {
	ClientID         string              `json:"client_id"`
	TaskStats        *TaskManagerStats   `json:"task_stats"`
	DecisionStats    *DecisionMakerStats `json:"decision_stats"`
	RateLimiterStats *RateLimiterStats   `json:"rate_limiter_stats"`
}

// PathRequest 路径请求
type PathRequest struct {
	SourceID      string `json:"source_id"`
	TargetAddress string `json:"target_address"`
	DataSize      int    `json:"data_size"`
	Priority      uint8  `json:"priority"`
	ChunkIndex    int    `json:"chunk_index"`
	TotalChunks   int    `json:"total_chunks"`
}

// PathResponse 路径响应
type PathResponse struct {
	Path            []string `json:"path"`             // 完整转发路径
	RecommendedRate float64  `json:"recommended_rate"` // 建议传输速率 (bytes/sec)
	TTL             int      `json:"ttl"`              // 路径有效期
	RequestID       string   `json:"request_id"`       // 请求ID
}
