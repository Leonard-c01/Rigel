package protocol

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/rigel/pkg/log"
)

// BlockHandler 数据块处理器
type BlockHandler struct {
	server       *BlockProtocolServer
	routeHandler RouteHandler
	stats        *BlockStats
	mu           sync.RWMutex
}

// RouteHandler 路由处理接口
type RouteHandler interface {
	// HandleBlock 处理数据块，返回目标连接
	HandleBlock(block *DataBlock) (net.Conn, error)
	// GetTargetConnection 根据路由信息获取目标连接
	GetTargetConnection(routeInfo RouteInfo) (net.Conn, error)
}

// BlockStats 数据块统计
type BlockStats struct {
	TotalBlocks     int64 `json:"total_blocks"`
	ProcessedBlocks int64 `json:"processed_blocks"`
	ErrorBlocks     int64 `json:"error_blocks"`
	TotalBytes      int64 `json:"total_bytes"`
	ProcessedBytes  int64 `json:"processed_bytes"`
}

// NewBlockHandler 创建数据块处理器
func NewBlockHandler(routeHandler RouteHandler) *BlockHandler {
	return &BlockHandler{
		server:       NewBlockProtocolServer(),
		routeHandler: routeHandler,
		stats:        &BlockStats{},
	}
}

// HandleConnection 处理客户端连接
func (bh *BlockHandler) HandleConnection(clientConn net.Conn) error {
	defer clientConn.Close()

	log.Infof("Handling block protocol connection from %s", clientConn.RemoteAddr())

	buffer := make([]byte, 64*1024) // 64KB缓冲区

	for {
		// 读取数据
		n, err := clientConn.Read(buffer)
		if err != nil {
			if err.Error() != "EOF" {
				log.Errorf("Error reading from client: %v", err)
			}
			break
		}

		// 处理接收到的数据
		blocks, err := bh.server.ProcessIncomingData(buffer[:n])
		if err != nil {
			log.Errorf("Error processing incoming data: %v", err)
			bh.updateStats(0, 0, 1, 0)
			continue
		}

		// 处理每个完整的数据块
		for _, block := range blocks {
			if err := bh.processBlock(block); err != nil {
				log.Errorf("Error processing block %d: %v", block.Header.BlockID, err)
				bh.updateStats(0, 0, 1, 0)
			} else {
				bh.updateStats(1, 1, 0, int64(len(block.Data)))
				log.Debugf("Successfully processed block %d to %s",
					block.Header.BlockID, block.Header.RouteInfo.TargetAddress)
			}
		}
	}

	return nil
}

// processBlock 处理单个数据块
func (bh *BlockHandler) processBlock(block *DataBlock) error {
	// 提取路由信息
	routeInfo := block.Header.RouteInfo

	log.Debugf("Processing block %d: target=%s, priority=%d, size=%d",
		block.Header.BlockID, routeInfo.TargetAddress, routeInfo.Priority, len(block.Data))

	// 检查TTL
	if routeInfo.TTL == 0 {
		return fmt.Errorf("block TTL expired")
	}

	// 检查时间戳（可选的新鲜度检查）
	if time.Now().Unix()-routeInfo.Timestamp > 300 { // 5分钟超时
		log.Warnf("Block %d is stale (timestamp: %d)", block.Header.BlockID, routeInfo.Timestamp)
	}

	// 获取目标连接
	targetConn, err := bh.routeHandler.GetTargetConnection(routeInfo)
	if err != nil {
		return fmt.Errorf("failed to get target connection: %v", err)
	}
	defer targetConn.Close()

	// 转发数据（只转发数据部分，不包括协议头部）
	_, err = targetConn.Write(block.Data)
	if err != nil {
		return fmt.Errorf("failed to forward data: %v", err)
	}

	log.Debugf("Forwarded %d bytes to %s", len(block.Data), routeInfo.TargetAddress)
	return nil
}

// updateStats 更新统计信息
func (bh *BlockHandler) updateStats(total, processed, errors, bytes int64) {
	bh.mu.Lock()
	defer bh.mu.Unlock()

	bh.stats.TotalBlocks += total
	bh.stats.ProcessedBlocks += processed
	bh.stats.ErrorBlocks += errors
	bh.stats.TotalBytes += bytes
	bh.stats.ProcessedBytes += bytes
}

// GetStats 获取统计信息
func (bh *BlockHandler) GetStats() *BlockStats {
	bh.mu.RLock()
	defer bh.mu.RUnlock()

	return &BlockStats{
		TotalBlocks:     bh.stats.TotalBlocks,
		ProcessedBlocks: bh.stats.ProcessedBlocks,
		ErrorBlocks:     bh.stats.ErrorBlocks,
		TotalBytes:      bh.stats.TotalBytes,
		ProcessedBytes:  bh.stats.ProcessedBytes,
	}
}

// SimpleRouteHandler 简单路由处理器实现
type SimpleRouteHandler struct {
	connections map[string]net.Conn
	mu          sync.RWMutex
}

// NewSimpleRouteHandler 创建简单路由处理器
func NewSimpleRouteHandler() *SimpleRouteHandler {
	return &SimpleRouteHandler{
		connections: make(map[string]net.Conn),
	}
}

// HandleBlock 处理数据块
func (srh *SimpleRouteHandler) HandleBlock(block *DataBlock) (net.Conn, error) {
	return srh.GetTargetConnection(block.Header.RouteInfo)
}

// GetTargetConnection 获取目标连接
func (srh *SimpleRouteHandler) GetTargetConnection(routeInfo RouteInfo) (net.Conn, error) {
	target := routeInfo.TargetAddress

	// 尝试复用现有连接
	srh.mu.RLock()
	if conn, exists := srh.connections[target]; exists {
		srh.mu.RUnlock()
		return conn, nil
	}
	srh.mu.RUnlock()

	// 创建新连接
	conn, err := net.DialTimeout("tcp", target, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %v", target, err)
	}

	// 缓存连接
	srh.mu.Lock()
	srh.connections[target] = conn
	srh.mu.Unlock()

	log.Debugf("Created new connection to %s", target)
	return conn, nil
}

// Close 关闭所有连接
func (srh *SimpleRouteHandler) Close() error {
	srh.mu.Lock()
	defer srh.mu.Unlock()

	for target, conn := range srh.connections {
		if err := conn.Close(); err != nil {
			log.Errorf("Error closing connection to %s: %v", target, err)
		}
	}

	srh.connections = make(map[string]net.Conn)
	return nil
}

// BlockProtocolProxy 基于块协议的代理
type BlockProtocolProxy struct {
	listener     net.Listener
	blockHandler *BlockHandler
	routeHandler *SimpleRouteHandler
}

// NewBlockProtocolProxy 创建基于块协议的代理
func NewBlockProtocolProxy(listenAddr string) (*BlockProtocolProxy, error) {
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %v", listenAddr, err)
	}

	routeHandler := NewSimpleRouteHandler()
	blockHandler := NewBlockHandler(routeHandler)

	return &BlockProtocolProxy{
		listener:     listener,
		blockHandler: blockHandler,
		routeHandler: routeHandler,
	}, nil
}

// Start 启动代理服务
func (bpp *BlockProtocolProxy) Start() error {
	log.Infof("Block protocol proxy listening on %s", bpp.listener.Addr())

	for {
		conn, err := bpp.listener.Accept()
		if err != nil {
			log.Errorf("Error accepting connection: %v", err)
			continue
		}

		// 为每个连接启动处理协程
		go func(clientConn net.Conn) {
			if err := bpp.blockHandler.HandleConnection(clientConn); err != nil {
				log.Errorf("Error handling connection from %s: %v",
					clientConn.RemoteAddr(), err)
			}
		}(conn)
	}
}

// Stop 停止代理服务
func (bpp *BlockProtocolProxy) Stop() error {
	if bpp.listener != nil {
		bpp.listener.Close()
	}

	if bpp.routeHandler != nil {
		bpp.routeHandler.Close()
	}

	return nil
}

// GetStats 获取代理统计信息
func (bpp *BlockProtocolProxy) GetStats() *BlockStats {
	return bpp.blockHandler.GetStats()
}
