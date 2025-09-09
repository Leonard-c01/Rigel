package dataplane

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"tcp-proxy/internal/hybrid"
	"tcp-proxy/internal/protocol"
	"tcp-proxy/pkg/log"
)

// DataPlaneForwarder 数据面转发器
type DataPlaneForwarder struct {
	// 节点配置
	nodeAddress string
	listenPort  string

	// 网络组件
	listener net.Listener

	// 高性能连接管理器缓存 (用于代理节点间通信)
	connectionManagers map[string]*hybrid.UnifiedConnectionManager
	connectionConfig   *hybrid.UnifiedConnectionConfig

	// 数据处理组件
	blockServer *protocol.BlockProtocolServer

	// 统计信息
	totalConnections  int64
	activeConnections int64
	totalForwards     int64
	totalBytes        int64
	errorCount        int64

	// 控制
	running bool
	mu      sync.RWMutex
}

// ForwarderStats 转发器统计信息
type ForwarderStats struct {
	NodeAddress       string `json:"node_address"`
	TotalConnections  int64  `json:"total_connections"`
	ActiveConnections int64  `json:"active_connections"`
	TotalForwards     int64  `json:"total_forwards"`
	TotalBytes        int64  `json:"total_bytes"`
	ErrorCount        int64  `json:"error_count"`
	Uptime            string `json:"uptime"`
}

// NewDataPlaneForwarder 创建数据面转发器
func NewDataPlaneForwarder(nodeAddress, listenPort string) *DataPlaneForwarder {
	// 创建统一连接管理器配置
	config := &hybrid.UnifiedConnectionConfig{
		MaxConnections:          3,                // 3个并行TCP连接
		SplitStrategy:           "round_robin",    // 轮询分发策略
		MaxStreamsPerConnection: 10,               // 每个连接最大10个流
		StreamBufferSize:        32 * 1024,        // 32KB流缓冲区
		KeepAliveInterval:       30 * time.Second, // 30秒心跳间隔
		KeepAliveTimeout:        90 * time.Second, // 90秒心跳超时
		ConnectionBufferSize:    64 * 1024,        // 64KB连接缓冲区
		ConnectionTimeout:       10 * time.Second, // 10秒连接超时
		RetryInterval:           2 * time.Second,  // 2秒重试间隔
		MaxRetries:              3,                // 最大重试3次
	}

	return &DataPlaneForwarder{
		nodeAddress:        nodeAddress,
		listenPort:         listenPort,
		connectionManagers: make(map[string]*hybrid.UnifiedConnectionManager),
		connectionConfig:   config,
		blockServer:        protocol.NewBlockProtocolServer(),
	}
}

// Start 启动转发器
func (dpf *DataPlaneForwarder) Start() error {
	dpf.mu.Lock()
	defer dpf.mu.Unlock()

	if dpf.running {
		return fmt.Errorf("forwarder already running")
	}

	// 创建监听器
	listener, err := net.Listen("tcp", dpf.listenPort)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", dpf.listenPort, err)
	}

	dpf.listener = listener
	dpf.running = true

	log.Infof("Data plane forwarder started: node=%s, listen=%s",
		dpf.nodeAddress, dpf.listenPort)

	// 启动接受连接的协程
	go dpf.acceptConnections()

	return nil
}

// Stop 停止转发器
func (dpf *DataPlaneForwarder) Stop() error {
	dpf.mu.Lock()
	defer dpf.mu.Unlock()

	if !dpf.running {
		return nil
	}

	dpf.running = false

	if dpf.listener != nil {
		dpf.listener.Close()
	}

	// 关闭所有连接管理器
	for target, manager := range dpf.connectionManagers {
		if err := manager.Close(); err != nil {
			log.Errorf("Error closing connection manager for %s: %v", target, err)
		}
	}
	dpf.connectionManagers = make(map[string]*hybrid.UnifiedConnectionManager)

	log.Infof("Data plane forwarder stopped: node=%s", dpf.nodeAddress)
	return nil
}

// acceptConnections 接受连接
func (dpf *DataPlaneForwarder) acceptConnections() {
	for dpf.running {
		conn, err := dpf.listener.Accept()
		if err != nil {
			if dpf.running {
				log.Errorf("Accept connection failed: %v", err)
				atomic.AddInt64(&dpf.errorCount, 1)
			}
			continue
		}

		atomic.AddInt64(&dpf.totalConnections, 1)
		atomic.AddInt64(&dpf.activeConnections, 1)

		// 为每个连接启动处理协程
		go dpf.handleConnection(conn)
	}
}

// handleConnection 处理单个连接
func (dpf *DataPlaneForwarder) handleConnection(clientConn net.Conn) {
	defer func() {
		clientConn.Close()
		atomic.AddInt64(&dpf.activeConnections, -1)
	}()

	log.Debugf("Handling connection from %s", clientConn.RemoteAddr())

	// 使用缓冲区机制处理TCP流
	buffer := make([]byte, 64*1024) // 64KB读取缓冲区

	for {
		// 从连接读取数据
		n, err := clientConn.Read(buffer)
		if err != nil {
			if err.Error() != "EOF" {
				log.Errorf("Failed to read from client: %v", err)
				atomic.AddInt64(&dpf.errorCount, 1)
			}
			break
		}

		if n == 0 {
			continue
		}

		// 将数据放入协议服务器的缓冲区
		atomic.AddInt64(&dpf.totalBytes, int64(n))

		// 处理接收到的数据，可能得到多个完整的数据块
		blocks, err := dpf.blockServer.ProcessIncomingData(buffer[:n])
		if err != nil {
			log.Errorf("Failed to process incoming data: %v", err)
			atomic.AddInt64(&dpf.errorCount, 1)
			continue
		}

		// 处理每个完整的数据块
		for _, block := range blocks {
			if err := dpf.processDataBlock(block); err != nil {
				log.Errorf("Failed to process data block %d: %v", block.Header.BlockID, err)
				atomic.AddInt64(&dpf.errorCount, 1)
			} else {
				atomic.AddInt64(&dpf.totalForwards, 1)
				log.Debugf("Successfully processed data block %d from %s",
					block.Header.BlockID, clientConn.RemoteAddr())
			}
		}
	}
}

// processDataBlock 处理数据块
func (dpf *DataPlaneForwarder) processDataBlock(block *protocol.DataBlock) error {
	// 提取头部和路径信息
	header := &block.Header
	pathString := header.RouteInfo.TargetAddress

	log.Debugf("Processing block: ID=%d, path=%s, current_node=%s",
		header.BlockID, pathString, dpf.nodeAddress)

	// 验证路径
	if err := protocol.ValidatePath(pathString); err != nil {
		return fmt.Errorf("invalid path: %v", err)
	}

	// 序列化数据块用于转发
	serializedData, err := block.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize block: %v", err)
	}

	// 检查是否到达终点
	if protocol.IsPathComplete(pathString, dpf.nodeAddress) {
		log.Infof("Reached final destination: block_id=%d", header.BlockID)
		return dpf.deliverToFinalTarget(serializedData, header)
	}

	// 获取下一跳
	nextHop, err := protocol.GetNextHop(pathString, dpf.nodeAddress)
	if err != nil {
		return fmt.Errorf("failed to get next hop: %v", err)
	}

	if nextHop == "" {
		log.Infof("No next hop, treating as final destination: block_id=%d", header.BlockID)
		return dpf.deliverToFinalTarget(serializedData, header)
	}

	log.Debugf("Forwarding to next hop: %s", nextHop)

	// 转发到下一跳
	return dpf.forwardToNextHop(serializedData, nextHop, header.BlockID)
}

// getOrCreateConnectionManager 获取或创建连接管理器
func (dpf *DataPlaneForwarder) getOrCreateConnectionManager(target string) (*hybrid.UnifiedConnectionManager, error) {
	dpf.mu.Lock()
	defer dpf.mu.Unlock()

	// 检查是否已存在
	if manager, exists := dpf.connectionManagers[target]; exists {
		return manager, nil
	}

	// 创建新的连接管理器
	manager := hybrid.NewUnifiedConnectionManager(target, dpf.connectionConfig)

	// 建立连接
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := manager.Connect(ctx, target)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %v", target, err)
	}

	// 缓存管理器
	dpf.connectionManagers[target] = manager

	log.Infof("Created hybrid connection manager for %s (parallel TCP + smux)", target)
	return manager, nil
}

// forwardToNextHop 转发到下一跳 (使用高性能连接)
func (dpf *DataPlaneForwarder) forwardToNextHop(data []byte, nextHop string, blockID uint64) error {
	log.Debugf("Forwarding block %d to %s using hybrid connection (%d bytes)",
		blockID, nextHop, len(data))

	// 获取或创建到目标的连接管理器
	manager, err := dpf.getOrCreateConnectionManager(nextHop)
	if err != nil {
		return fmt.Errorf("failed to get connection manager for %s: %v", nextHop, err)
	}

	// 使用统一连接管理器进行高性能转发
	err = manager.Send(data)
	if err != nil {
		return fmt.Errorf("failed to forward data to %s via hybrid connection: %v", nextHop, err)
	}

	log.Debugf("Successfully forwarded block %d to %s via hybrid connection (%d bytes)",
		blockID, nextHop, len(data))

	return nil
}

// deliverToFinalTarget 交付到最终目标
func (dpf *DataPlaneForwarder) deliverToFinalTarget(data []byte, header *protocol.BlockHeader) error {
	// 提取原始数据（去除协议头部）
	originalData := data[protocol.BlockHeaderSize:]

	log.Infof("Delivering to final target: block_id=%d, data_size=%d",
		header.BlockID, len(originalData))

	// 这里可以根据需要处理最终数据
	// 例如：保存到文件、发送到应用程序、返回响应等

	log.Infof("Final delivery completed: block_id=%d, original_data=%s",
		header.BlockID, string(originalData))

	return nil
}

// GetStats 获取统计信息
func (dpf *DataPlaneForwarder) GetStats() *ForwarderStats {
	return &ForwarderStats{
		NodeAddress:       dpf.nodeAddress,
		TotalConnections:  atomic.LoadInt64(&dpf.totalConnections),
		ActiveConnections: atomic.LoadInt64(&dpf.activeConnections),
		TotalForwards:     atomic.LoadInt64(&dpf.totalForwards),
		TotalBytes:        atomic.LoadInt64(&dpf.totalBytes),
		ErrorCount:        atomic.LoadInt64(&dpf.errorCount),
		Uptime:            dpf.getUptime(),
	}
}

// getUptime 获取运行时间
func (dpf *DataPlaneForwarder) getUptime() string {
	// 简化实现，实际应该记录启动时间
	return "unknown"
}

// IsRunning 检查是否运行中
func (dpf *DataPlaneForwarder) IsRunning() bool {
	dpf.mu.RLock()
	defer dpf.mu.RUnlock()
	return dpf.running
}
