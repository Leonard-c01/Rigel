package multiplex

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rigel/internal/parallel"
	"github.com/rigel/pkg/log"
)

// HybridConnectionConfig 混合连接配置
type HybridConnectionConfig struct {
	// 并行连接配置
	ParallelConfig *parallel.ConnectionConfig `json:"parallel_config"`

	// 多路复用配置
	MultiplexConfig *MultiplexConfig `json:"multiplex_config"`

	// 混合模式配置
	Mode                ConnectionMode `json:"mode"`                 // 连接模式
	BandwidthThreshold  int64          `json:"bandwidth_threshold"`  // 带宽阈值（bps）
	LatencyThreshold    int            `json:"latency_threshold"`    // 延迟阈值（ms）
	ConnectionThreshold int            `json:"connection_threshold"` // 连接数阈值

	// 自动切换配置
	AutoSwitchEnabled bool          `json:"auto_switch_enabled"` // 是否启用自动切换
	SwitchInterval    time.Duration `json:"switch_interval"`     // 切换检查间隔
	PerformanceWindow time.Duration `json:"performance_window"`  // 性能统计窗口
}

// DefaultHybridConnectionConfig 默认混合连接配置
func DefaultHybridConnectionConfig() *HybridConnectionConfig {
	return &HybridConnectionConfig{
		ParallelConfig:      parallel.DefaultConnectionConfig(),
		MultiplexConfig:     DefaultMultiplexConfig(),
		Mode:                ModeAuto,
		BandwidthThreshold:  5000000, // 5Mbps
		LatencyThreshold:    100,     // 100ms
		ConnectionThreshold: 50,      // 50个并发连接
		AutoSwitchEnabled:   true,
		SwitchInterval:      30 * time.Second,
		PerformanceWindow:   5 * time.Minute,
	}
}

// HybridConnection 混合连接管理器
type HybridConnection struct {
	target string
	config *HybridConnectionConfig

	// 连接实例
	parallelConn  parallel.ParallelConnection
	multiplexConn MultiplexConnection
	transport     *MultiplexTransport

	// 当前模式
	currentMode ConnectionMode

	// 性能统计
	parallelStats  *parallel.ParallelStats
	multiplexStats *MultiplexStats

	// 统计信息
	totalBytesSent int64
	totalBytesRecv int64
	switchCount    int64

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.RWMutex
	wg     sync.WaitGroup
}

// NewHybridConnection 创建混合连接管理器
func NewHybridConnection(target string, config *HybridConnectionConfig) *HybridConnection {
	if config == nil {
		config = DefaultHybridConnectionConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	hc := &HybridConnection{
		target:      target,
		config:      config,
		currentMode: config.Mode,
		ctx:         ctx,
		cancel:      cancel,
	}

	// 初始化连接实例
	hc.parallelConn = parallel.NewTCPParallelConnection(target, config.ParallelConfig)
	hc.multiplexConn = NewTCPMultiplexConnection(target, config.MultiplexConfig)
	hc.transport = NewMultiplexTransport(target, config.MultiplexConfig)

	return hc
}

// Connect 建立混合连接
func (hc *HybridConnection) Connect(ctx context.Context, target string) error {
	log.Infof("Establishing hybrid connection to %s with mode %s", target, hc.currentMode)

	hc.target = target

	// 根据模式建立连接
	switch hc.currentMode {
	case ModeParallelOnly:
		return hc.connectParallel(ctx)
	case ModeMultiplexOnly:
		return hc.connectMultiplex(ctx)
	case ModeHybrid:
		return hc.connectHybrid(ctx)
	case ModeAuto:
		return hc.connectAuto(ctx)
	default:
		return fmt.Errorf("unsupported connection mode: %s", hc.currentMode)
	}
}

// Send 发送数据
func (hc *HybridConnection) Send(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty data")
	}

	var err error
	switch hc.getCurrentMode() {
	case ModeParallelOnly:
		err = hc.parallelConn.Send(data)
	case ModeMultiplexOnly:
		err = hc.transport.SendData(data)
	case ModeHybrid:
		err = hc.sendHybrid(data)
	default:
		err = fmt.Errorf("unsupported mode for sending: %s", hc.getCurrentMode())
	}

	if err == nil {
		atomic.AddInt64(&hc.totalBytesSent, int64(len(data)))
	}

	return err
}

// Receive 接收数据
func (hc *HybridConnection) Receive() ([]byte, error) {
	var data []byte
	var err error

	switch hc.getCurrentMode() {
	case ModeParallelOnly:
		data, err = hc.parallelConn.Receive()
	case ModeMultiplexOnly:
		data, err = hc.transport.ReceiveData()
	case ModeHybrid:
		data, err = hc.receiveHybrid()
	default:
		err = fmt.Errorf("unsupported mode for receiving: %s", hc.getCurrentMode())
	}

	if err == nil && len(data) > 0 {
		atomic.AddInt64(&hc.totalBytesRecv, int64(len(data)))
	}

	return data, err
}

// ForwardData 双向数据转发
func (hc *HybridConnection) ForwardData(clientConn net.Conn) (int64, int64, error) {
	switch hc.getCurrentMode() {
	case ModeParallelOnly:
		return hc.forwardParallel(clientConn)
	case ModeMultiplexOnly:
		return hc.transport.ForwardData(clientConn)
	case ModeHybrid:
		return hc.forwardHybrid(clientConn)
	default:
		return 0, 0, fmt.Errorf("unsupported mode for forwarding: %s", hc.getCurrentMode())
	}
}

// Close 关闭混合连接
func (hc *HybridConnection) Close() error {
	log.Info("Closing hybrid connection")

	hc.cancel()
	hc.wg.Wait()

	var errors []error

	// 关闭并行连接
	if hc.parallelConn != nil {
		if err := hc.parallelConn.Close(); err != nil {
			errors = append(errors, fmt.Errorf("parallel connection close error: %v", err))
		}
	}

	// 关闭多路复用连接
	if hc.multiplexConn != nil {
		if err := hc.multiplexConn.Close(); err != nil {
			errors = append(errors, fmt.Errorf("multiplex connection close error: %v", err))
		}
	}

	// 关闭传输器
	if hc.transport != nil {
		if err := hc.transport.Close(); err != nil {
			errors = append(errors, fmt.Errorf("transport close error: %v", err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("close errors: %v", errors)
	}

	log.Info("Hybrid connection closed")
	return nil
}

// GetStats 获取统计信息
func (hc *HybridConnection) GetStats() *HybridStats {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	stats := &HybridStats{
		CurrentMode:    hc.currentMode,
		TotalBytesSent: atomic.LoadInt64(&hc.totalBytesSent),
		TotalBytesRecv: atomic.LoadInt64(&hc.totalBytesRecv),
		SwitchCount:    atomic.LoadInt64(&hc.switchCount),
	}

	if hc.parallelConn != nil {
		stats.ParallelStats = hc.parallelConn.GetStats()
	}

	if hc.transport != nil {
		stats.MultiplexStats = hc.transport.GetStats()
	}

	return stats
}

// connectParallel 建立并行连接
func (hc *HybridConnection) connectParallel(ctx context.Context) error {
	return hc.parallelConn.Connect(ctx, hc.target)
}

// connectMultiplex 建立多路复用连接
func (hc *HybridConnection) connectMultiplex(ctx context.Context) error {
	return hc.transport.Connect(ctx, hc.target)
}

// connectHybrid 建立混合连接
func (hc *HybridConnection) connectHybrid(ctx context.Context) error {
	// 同时建立两种连接
	if err := hc.connectParallel(ctx); err != nil {
		log.Warnf("Failed to establish parallel connection: %v", err)
	}

	if err := hc.connectMultiplex(ctx); err != nil {
		log.Warnf("Failed to establish multiplex connection: %v", err)
	}

	// 启动性能监控
	if hc.config.AutoSwitchEnabled {
		hc.wg.Add(1)
		go hc.performanceMonitor()
	}

	return nil
}

// connectAuto 自动选择连接模式
func (hc *HybridConnection) connectAuto(ctx context.Context) error {
	// 根据配置阈值选择初始模式
	mode := hc.selectOptimalMode()
	hc.setCurrentMode(mode)

	switch mode {
	case ModeParallelOnly:
		return hc.connectParallel(ctx)
	case ModeMultiplexOnly:
		return hc.connectMultiplex(ctx)
	default:
		return hc.connectHybrid(ctx)
	}
}

// getCurrentMode 获取当前模式
func (hc *HybridConnection) getCurrentMode() ConnectionMode {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.currentMode
}

// setCurrentMode 设置当前模式
func (hc *HybridConnection) setCurrentMode(mode ConnectionMode) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	if hc.currentMode != mode {
		log.Infof("Switching connection mode from %s to %s", hc.currentMode, mode)
		hc.currentMode = mode
		atomic.AddInt64(&hc.switchCount, 1)
	}
}

// selectOptimalMode 选择最优模式
func (hc *HybridConnection) selectOptimalMode() ConnectionMode {
	// 这里简化实现，实际应该根据网络条件、负载等因素决定
	// 可以集成到调度器的决策中
	return ModeMultiplexOnly // 默认使用多路复用
}

// HybridStats 混合连接统计
type HybridStats struct {
	CurrentMode    ConnectionMode          `json:"current_mode"`
	TotalBytesSent int64                   `json:"total_bytes_sent"`
	TotalBytesRecv int64                   `json:"total_bytes_recv"`
	SwitchCount    int64                   `json:"switch_count"`
	ParallelStats  *parallel.ParallelStats `json:"parallel_stats,omitempty"`
	MultiplexStats *MultiplexStats         `json:"multiplex_stats,omitempty"`
}

// sendHybrid 混合模式发送
func (hc *HybridConnection) sendHybrid(data []byte) error {
	// 简化实现：根据数据大小选择发送方式
	if len(data) > 64*1024 { // 大数据使用并行连接
		return hc.parallelConn.Send(data)
	} else { // 小数据使用多路复用
		return hc.transport.SendData(data)
	}
}

// receiveHybrid 混合模式接收
func (hc *HybridConnection) receiveHybrid() ([]byte, error) {
	// 简化实现：优先从多路复用接收
	return hc.transport.ReceiveData()
}

// forwardParallel 并行连接转发
func (hc *HybridConnection) forwardParallel(clientConn net.Conn) (int64, int64, error) {
	// 这里需要实现并行连接的转发逻辑
	// 简化实现
	return 0, 0, fmt.Errorf("parallel forwarding not implemented")
}

// forwardHybrid 混合模式转发
func (hc *HybridConnection) forwardHybrid(clientConn net.Conn) (int64, int64, error) {
	// 优先使用多路复用进行转发
	return hc.transport.ForwardData(clientConn)
}

// performanceMonitor 性能监控
func (hc *HybridConnection) performanceMonitor() {
	defer hc.wg.Done()

	ticker := time.NewTicker(hc.config.SwitchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-hc.ctx.Done():
			return
		case <-ticker.C:
			hc.evaluatePerformance()
		}
	}
}

// evaluatePerformance 评估性能并决定是否切换模式
func (hc *HybridConnection) evaluatePerformance() {
	// 获取当前统计信息
	stats := hc.GetStats()

	// 根据性能指标决定是否需要切换模式
	// 这里简化实现，实际应该有更复杂的决策逻辑

	log.Debugf("Performance evaluation: mode=%s, sent=%d, recv=%d, switches=%d",
		stats.CurrentMode, stats.TotalBytesSent, stats.TotalBytesRecv, stats.SwitchCount)
}
