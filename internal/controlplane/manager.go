package controlplane

import (
	"fmt"

	"github.com/rigel/pkg/log"
)

// ControlPlaneManager 控制平面管理器
type ControlPlaneManager struct {
	config *ControlPlaneConfig

	// 节点实例 (只有一个会被使用)
	leaderNode  *LeaderNode
	regularNode *RegularNode
}

// NewControlPlaneManager 创建控制平面管理器
func NewControlPlaneManager(config *ControlPlaneConfig) *ControlPlaneManager {
	if config == nil {
		config = DefaultControlPlaneConfig()
	}

	return &ControlPlaneManager{
		config: config,
	}
}

// Start 启动控制平面
func (cpm *ControlPlaneManager) Start() error {
	if cpm.config.IsLeader {
		return cpm.startAsLeader()
	} else {
		return cpm.startAsRegular()
	}
}

// Stop 停止控制平面
func (cpm *ControlPlaneManager) Stop() error {
	if cpm.leaderNode != nil {
		return cpm.leaderNode.Stop()
	}
	if cpm.regularNode != nil {
		return cpm.regularNode.Stop()
	}
	return nil
}

// startAsLeader 作为组长节点启动
func (cpm *ControlPlaneManager) startAsLeader() error {
	log.Infof("Starting control plane as leader node")

	cpm.leaderNode = NewLeaderNode(cpm.config)
	return cpm.leaderNode.Start()
}

// startAsRegular 作为普通节点启动
func (cpm *ControlPlaneManager) startAsRegular() error {
	log.Infof("Starting control plane as regular node")

	if cpm.config.LeaderAddress == "" {
		return fmt.Errorf("leader address is required for regular nodes")
	}

	cpm.regularNode = NewRegularNode(cpm.config)
	return cpm.regularNode.Start()
}

// GetGlobalState 获取全局网络状态
func (cpm *ControlPlaneManager) GetGlobalState() *GlobalNetworkState {
	if cpm.leaderNode != nil {
		return cpm.leaderNode.getGlobalState()
	}
	if cpm.regularNode != nil {
		return cpm.regularNode.GetGlobalState()
	}
	return nil
}

// UpdateVirtualQueue 更新虚拟队列值 (仅对普通节点有效)
func (cpm *ControlPlaneManager) UpdateVirtualQueue(linkID string, value float64) {
	if cpm.regularNode != nil {
		cpm.regularNode.UpdateVirtualQueue(linkID, value)
	}
}

// IsLeader 检查是否为组长节点
func (cpm *ControlPlaneManager) IsLeader() bool {
	return cpm.config.IsLeader
}

// GetConfig 获取配置
func (cpm *ControlPlaneManager) GetConfig() *ControlPlaneConfig {
	return cpm.config
}

// GetRegionNodes 获取区域节点列表 (仅对组长节点有效)
func (cpm *ControlPlaneManager) GetRegionNodes() map[string]NodeMember {
	if cpm.leaderNode != nil {
		return cpm.leaderNode.getRegionNodes()
	}
	return nil
}

// GetRegionSummary 获取区域摘要 (仅对组长节点有效)
func (cpm *ControlPlaneManager) GetRegionSummary() RegionSummary {
	if cpm.leaderNode != nil {
		return cpm.leaderNode.generateRegionSummary()
	}
	return RegionSummary{}
}
