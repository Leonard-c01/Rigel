package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rigel/internal/controlplane"
	"github.com/rigel/internal/dataplane"
	"github.com/rigel/pkg/log"

	"github.com/rigel/config"
)

var (
	configFile = flag.String("c", "config.yaml", "config file path")
)

func main() {
	flag.Parse()

	// 加载配置
	cfg, err := config.LoadConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 初始化日志
	if err := log.Init(cfg.Common.LogLevel, cfg.Common.LogFile); err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}

	log.Infof("Starting Rigel proxy node: %s", cfg.Common.NodeID)

	// 创建控制平面管理器
	controlPlaneConfig := convertToControlPlaneConfig(cfg)
	controlPlaneManager := controlplane.NewControlPlaneManager(controlPlaneConfig)

	// 启动控制平面
	if err := controlPlaneManager.Start(); err != nil {
		log.Fatalf("Failed to start control plane: %v", err)
	}

	// 创建数据面转发器
	nodeAddress := cfg.Common.NodeID + ":8080" // 默认端口
	forwarder := dataplane.NewDataPlaneForwarder(nodeAddress, ":8080")

	// 启动转发器
	if err := forwarder.Start(); err != nil {
		log.Fatalf("Failed to start data plane forwarder: %v", err)
	}

	// 等待信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// 停止服务
	log.Info("Shutting down...")

	// 停止控制平面
	if err := controlPlaneManager.Stop(); err != nil {
		log.Errorf("Error stopping control plane: %v", err)
	}

	// 停止转发器
	if err := forwarder.Stop(); err != nil {
		log.Errorf("Error stopping data plane forwarder: %v", err)
	}

	log.Info("Rigel proxy node stopped")
}

// convertToControlPlaneConfig 将配置转换为控制平面配置
func convertToControlPlaneConfig(cfg *config.Config) *controlplane.ControlPlaneConfig {
	cpConfig := &controlplane.ControlPlaneConfig{
		NodeID:  cfg.Common.NodeID,
		Region:  cfg.Common.Region,
		Address: cfg.ControlPlane.Address,
	}

	// 如果配置中没有指定地址，使用默认地址
	if cpConfig.Address == "" {
		cpConfig.Address = "127.0.0.1:9090"
	}

	// 复制控制平面配置
	cpConfig.IsLeader = cfg.ControlPlane.IsLeader
	cpConfig.LeaderAddress = cfg.ControlPlane.LeaderAddress
	cpConfig.LeaderNodes = cfg.ControlPlane.LeaderNodes
	cpConfig.StatusReportInterval = cfg.ControlPlane.StatusReportInterval
	cpConfig.GlobalSyncInterval = cfg.ControlPlane.GlobalSyncInterval
	cpConfig.LeaderSyncInterval = cfg.ControlPlane.LeaderSyncInterval
	cpConfig.HeartbeatTimeout = cfg.ControlPlane.HeartbeatTimeout
	cpConfig.RequestTimeout = cfg.ControlPlane.RequestTimeout
	cpConfig.APIPort = cfg.ControlPlane.APIPort

	// 设置默认值
	if cpConfig.StatusReportInterval == 0 {
		cpConfig.StatusReportInterval = 5 * time.Second
	}
	if cpConfig.GlobalSyncInterval == 0 {
		cpConfig.GlobalSyncInterval = 1 * time.Second
	}
	if cpConfig.LeaderSyncInterval == 0 {
		cpConfig.LeaderSyncInterval = 1 * time.Second
	}
	if cpConfig.HeartbeatTimeout == 0 {
		cpConfig.HeartbeatTimeout = 30 * time.Second
	}
	if cpConfig.RequestTimeout == 0 {
		cpConfig.RequestTimeout = 10 * time.Second
	}
	if cpConfig.APIPort == "" {
		cpConfig.APIPort = ":9090"
	}

	return cpConfig
}
