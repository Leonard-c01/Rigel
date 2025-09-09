package controlplane

import (
	"fmt"
	"sync"
	"time"
)

// NodeStatus 节点状态信息
type NodeStatus struct {
	NodeID    string    `json:"node_id"`
	Region    string    `json:"region"`
	Address   string    `json:"address"`
	Timestamp time.Time `json:"timestamp"`

	// 系统资源状态
	CPUUsage    float64 `json:"cpu_usage"`    // CPU使用率 (0.0-1.0)
	MemoryUsage float64 `json:"memory_usage"` // 内存使用率 (0.0-1.0)

	// 网络状态
	InboundBandwidth  int64 `json:"inbound_bandwidth"`  // 入站带宽 (bytes/sec)
	OutboundBandwidth int64 `json:"outbound_bandwidth"` // 出站带宽 (bytes/sec)

	// 虚拟队列状态 - 关键指标
	VirtualQueues map[string]float64 `json:"virtual_queues"` // 出向链路的虚拟队列状态 Õe(t)

	// 健康状态
	IsHealthy bool `json:"is_healthy"`
}

// RegionSummary 区域摘要信息
type RegionSummary struct {
	RegionID  string    `json:"region_id"`
	Timestamp time.Time `json:"timestamp"`

	// 区域统计
	TotalNodes   int `json:"total_nodes"`
	HealthyNodes int `json:"healthy_nodes"`

	// 区域间链路质量
	InterRegionLinks map[string]LinkQuality `json:"inter_region_links"`

	// 区域总体负载
	AverageCPUUsage    float64 `json:"average_cpu_usage"`
	AverageMemoryUsage float64 `json:"average_memory_usage"`
	TotalBandwidth     int64   `json:"total_bandwidth"`

	// 虚拟队列摘要
	AverageVirtualQueue float64            `json:"average_virtual_queue"`
	VirtualQueueSummary map[string]float64 `json:"virtual_queue_summary"`
}

// LinkQuality 链路质量信息
type LinkQuality struct {
	TargetRegion string  `json:"target_region"`
	Latency      float64 `json:"latency"`     // 延迟 (ms)
	Bandwidth    int64   `json:"bandwidth"`   // 带宽 (bytes/sec)
	PacketLoss   float64 `json:"packet_loss"` // 丢包率 (0.0-1.0)
	Reliability  float64 `json:"reliability"` // 可靠性评分 (0.0-1.0)
}

// GlobalNetworkState 全局网络状态视图
type GlobalNetworkState struct {
	Timestamp time.Time `json:"timestamp"`

	// 本区域详细信息
	LocalRegion string                `json:"local_region"`
	LocalNodes  map[string]NodeStatus `json:"local_nodes"`

	// 其他区域摘要信息
	RemoteRegions map[string]RegionSummary `json:"remote_regions"`

	// 全局统计
	TotalNodes   int `json:"total_nodes"`
	TotalRegions int `json:"total_regions"`
}

// NodeMember 成员节点信息
type NodeMember struct {
	NodeID    string     `json:"node_id"`
	Address   string     `json:"address"`
	Region    string     `json:"region"`
	IsHealthy bool       `json:"is_healthy"`
	LastSeen  time.Time  `json:"last_seen"`
	JoinedAt  time.Time  `json:"joined_at"`
	Status    NodeStatus `json:"status"`
	mu        sync.RWMutex
}

// UpdateLastSeen 更新最后见到时间
func (nm *NodeMember) UpdateLastSeen() {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	nm.LastSeen = time.Now()
}

// SetHealthy 设置健康状态
func (nm *NodeMember) SetHealthy(healthy bool) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	nm.IsHealthy = healthy
}

// GetLastSeen 获取最后见到时间
func (nm *NodeMember) GetLastSeen() time.Time {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	return nm.LastSeen
}

// IsExpired 检查是否过期
func (nm *NodeMember) IsExpired(timeout time.Duration) bool {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	return time.Since(nm.LastSeen) > timeout
}

// StatusReportRequest 状态上报请求
type StatusReportRequest struct {
	NodeStatus NodeStatus `json:"node_status"`
}

// StatusReportResponse 状态上报响应
type StatusReportResponse struct {
	Success   bool      `json:"success"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}

// GlobalStatusResponse 全局状态响应
type GlobalStatusResponse struct {
	GlobalState GlobalNetworkState `json:"global_state"`
	Success     bool               `json:"success"`
	Message     string             `json:"message"`
}

// RegionSummaryResponse 区域摘要响应
type RegionSummaryResponse struct {
	Summary RegionSummary `json:"summary"`
	Success bool          `json:"success"`
	Message string        `json:"message"`
}

// RegionNodesResponse 区域节点列表响应
type RegionNodesResponse struct {
	Nodes   map[string]NodeMember `json:"nodes"`
	Success bool                  `json:"success"`
	Message string                `json:"message"`
}

// ControlPlaneConfig 控制平面配置
type ControlPlaneConfig struct {
	// 节点配置
	NodeID   string `json:"node_id"`
	Region   string `json:"region"`
	Address  string `json:"address"`
	IsLeader bool   `json:"is_leader"`

	// 组长节点配置
	LeaderAddress string   `json:"leader_address"`
	LeaderNodes   []string `json:"leader_nodes"` // 其他区域的组长节点地址

	// 同步配置
	StatusReportInterval time.Duration `json:"status_report_interval"` // 状态上报间隔 (默认5秒)
	GlobalSyncInterval   time.Duration `json:"global_sync_interval"`   // 全局同步间隔 (默认1秒)
	LeaderSyncInterval   time.Duration `json:"leader_sync_interval"`   // 组长间同步间隔 (默认1秒)

	// 超时配置
	HeartbeatTimeout time.Duration `json:"heartbeat_timeout"` // 心跳超时 (默认30秒)
	RequestTimeout   time.Duration `json:"request_timeout"`   // 请求超时 (默认10秒)

	// API配置
	APIPort string `json:"api_port"` // API服务端口 (默认:9090)
}

// DefaultControlPlaneConfig 默认控制平面配置
func DefaultControlPlaneConfig() *ControlPlaneConfig {
	return &ControlPlaneConfig{
		StatusReportInterval: 5 * time.Second,
		GlobalSyncInterval:   1 * time.Second,
		LeaderSyncInterval:   1 * time.Second,
		HeartbeatTimeout:     30 * time.Second,
		RequestTimeout:       10 * time.Second,
		APIPort:              ":9090",
	}
}

// UMW 路由优化相关类型定义

// RoutingRequest 路由请求
type RoutingRequest struct {
	TaskID        string  `json:"task_id"`
	SourceID      string  `json:"source_id"`
	DestinationID string  `json:"destination_id"`
	DataSize      int64   `json:"data_size"`
	Priority      float64 `json:"priority"`       // w_k^U 任务权重
	FairnessAlpha float64 `json:"fairness_alpha"` // α 公平性参数
	Timestamp     int64   `json:"timestamp"`
}

// RoutingResponse 路由响应
type RoutingResponse struct {
	TaskID           string   `json:"task_id"`
	OptimalPath      []string `json:"optimal_path"`      // 最优路径节点列表
	RecommendedRate  float64  `json:"recommended_rate"`  // 最优传输速率 (bytes/sec)
	PathCost         float64  `json:"path_cost"`         // 路径总成本
	ComputationTime  float64  `json:"computation_time"`  // 计算时间 (ms)
	AlgorithmVersion string   `json:"algorithm_version"` // 算法版本
	Timestamp        int64    `json:"timestamp"`
}

// ExtendedGraph 扩展有向图 G'=(V', E')
type ExtendedGraph struct {
	Nodes map[string]*ExtendedNode `json:"nodes"`
	Edges map[string]*ExtendedEdge `json:"edges"`
	mu    sync.RWMutex
}

// ExtendedNode 扩展节点 (包含输入和输出组件)
type ExtendedNode struct {
	NodeID       string `json:"node_id"`
	InputNodeID  string `json:"input_node_id"`  // iv
	OutputNodeID string `json:"output_node_id"` // jo
	Region       string `json:"region"`
	Address      string `json:"address"`
	IsVirtual    bool   `json:"is_virtual"` // 是否为虚拟节点
}

// ExtendedEdge 扩展边
type ExtendedEdge struct {
	EdgeID        string  `json:"edge_id"`
	SourceNodeID  string  `json:"source_node_id"`
	TargetNodeID  string  `json:"target_node_id"`
	Capacity      float64 `json:"capacity"`        // be(t) 链路容量
	UnitCost      float64 `json:"unit_cost"`       // pe 单位传输成本
	VirtualQueue  float64 `json:"virtual_queue"`   // Õe(t) 虚拟队列值
	IsVirtualEdge bool    `json:"is_virtual_edge"` // 是否为节点内部虚拟边
	LastUpdated   int64   `json:"last_updated"`
}

// UMWConfig UMW算法配置参数
type UMWConfig struct {
	WeightM       float64 `json:"weight_m"`       // w_e^M 稳定性权重参数
	PenaltyV      float64 `json:"penalty_v"`      // V_M Drift-Plus-Penalty参数
	DefaultAlpha  float64 `json:"default_alpha"`  // 默认公平性参数
	MaxIterations int     `json:"max_iterations"` // 最大迭代次数
	Tolerance     float64 `json:"tolerance"`      // 收敛容差
}

// DefaultUMWConfig 默认UMW配置
func DefaultUMWConfig() *UMWConfig {
	return &UMWConfig{
		WeightM:       1.0,
		PenaltyV:      1.0,
		DefaultAlpha:  0.5,
		MaxIterations: 1000,
		Tolerance:     1e-6,
	}
}

// ValidateUMWConfig 验证UMW配置参数
func ValidateUMWConfig(config *UMWConfig) error {
	if config == nil {
		return fmt.Errorf("UMW config cannot be nil")
	}

	if config.WeightM < 0 {
		return fmt.Errorf("WeightM must be non-negative, got: %f", config.WeightM)
	}

	if config.PenaltyV <= 0 {
		return fmt.Errorf("PenaltyV must be positive, got: %f", config.PenaltyV)
	}

	if config.DefaultAlpha < 0 || config.DefaultAlpha > 1 {
		return fmt.Errorf("DefaultAlpha must be in range [0, 1], got: %f", config.DefaultAlpha)
	}

	if config.MaxIterations <= 0 {
		return fmt.Errorf("MaxIterations must be positive, got: %d", config.MaxIterations)
	}

	if config.Tolerance <= 0 {
		return fmt.Errorf("Tolerance must be positive, got: %f", config.Tolerance)
	}

	return nil
}

// ValidateRoutingRequest 验证路由请求参数
func ValidateRoutingRequest(request *RoutingRequest) error {
	if request == nil {
		return fmt.Errorf("routing request cannot be nil")
	}

	if request.SourceID == "" {
		return fmt.Errorf("source ID cannot be empty")
	}

	if request.DestinationID == "" {
		return fmt.Errorf("destination ID cannot be empty")
	}

	if request.SourceID == request.DestinationID {
		return fmt.Errorf("source and destination cannot be the same: %s", request.SourceID)
	}

	if request.DataSize < 0 {
		return fmt.Errorf("data size must be non-negative, got: %d", request.DataSize)
	}

	if request.Priority <= 0 {
		return fmt.Errorf("priority must be positive, got: %f", request.Priority)
	}

	if request.FairnessAlpha < 0 || request.FairnessAlpha > 1 {
		return fmt.Errorf("fairness alpha must be in range [0, 1], got: %f", request.FairnessAlpha)
	}

	return nil
}

// ===== 弹性伸缩相关类型定义 =====

// ElasticNodeState 弹性节点状态枚举
type ElasticNodeState string

const (
	// UNSCALED 状态 (未伸缩)
	StateInactive  ElasticNodeState = "INACTIVE"   // 未激活状态
	StateScalingUp ElasticNodeState = "SCALING_UP" // 正在启动中
	StateReleasing ElasticNodeState = "RELEASING"  // 正在释放中

	// SCALED 状态 (已伸缩)
	StateDormant   ElasticNodeState = "DORMANT"   // 休眠状态（已分配但未使用）
	StateTriggered ElasticNodeState = "TRIGGERED" // 已激活状态
	StatePermanent ElasticNodeState = "PERMANENT" // 永久状态
)

// ElasticNodeInfo 弹性节点信息
type ElasticNodeInfo struct {
	NodeID        string           `json:"node_id"`
	State         ElasticNodeState `json:"state"`
	Region        string           `json:"region"`
	CloudProvider string           `json:"cloud_provider"` // AWS, VULTR, etc.
	InstanceID    string           `json:"instance_id"`    // 云实例ID
	InstanceType  string           `json:"instance_type"`  // 实例类型

	// 波动性队列相关
	VolatilityQueue float64 `json:"volatility_queue"` // Zi(t)
	Perturbation    float64 `json:"perturbation"`     // Pi(t)

	// 时间相关
	CreatedAt       time.Time `json:"created_at"`
	LastTriggeredAt time.Time `json:"last_triggered_at"`
	RetainTime      float64   `json:"retain_time"`    // ri(t) 保留时间（秒）
	ActivityScore   float64   `json:"activity_score"` // 活动分数

	// 成本相关
	FixedCost    float64 `json:"fixed_cost"`    // C_fixed,i
	VariableCost float64 `json:"variable_cost"` // βi

	// 状态转换历史
	StateHistory []StateTransition `json:"state_history"`
}

// StateTransition 状态转换记录
type StateTransition struct {
	FromState ElasticNodeState `json:"from_state"`
	ToState   ElasticNodeState `json:"to_state"`
	Timestamp time.Time        `json:"timestamp"`
	Reason    string           `json:"reason"`
}

// BPScalerConfig BP-Scaler配置
type BPScalerConfig struct {
	// 波动性队列参数
	CongestionThreshold float64 `json:"congestion_threshold"` // CV 拥塞阈值
	DecayFactor         float64 `json:"decay_factor"`         // γi 衰减因子
	SensitivityWeight   float64 `json:"sensitivity_weight"`   // w_i^V 敏感度权重
	StabilityCapacity   float64 `json:"stability_capacity"`   // Ci(t) 稳定能力

	// 决策参数
	ScalingWeight float64 `json:"scaling_weight"` // w_i^S 伸缩权重
	CostWeight    float64 `json:"cost_weight"`    // V_S 成本权重

	// 保留时间参数
	BaseRetainTime     float64 `json:"base_retain_time"`    // T0 基础保留时间（秒）
	ActivityDecayRate  float64 `json:"activity_decay_rate"` // 活动分数衰减率
	PermanentThreshold float64 `json:"permanent_threshold"` // T_threshold 永久状态阈值（秒）

	// 决策周期
	DecisionInterval   time.Duration `json:"decision_interval"`   // 决策周期
	MonitoringInterval time.Duration `json:"monitoring_interval"` // 监控周期
}

// DefaultBPScalerConfig 默认BP-Scaler配置
func DefaultBPScalerConfig() *BPScalerConfig {
	return &BPScalerConfig{
		CongestionThreshold: 1.0,
		DecayFactor:         0.9,
		SensitivityWeight:   1.0,
		StabilityCapacity:   0.5,
		ScalingWeight:       1.0,
		CostWeight:          0.1,
		BaseRetainTime:      300.0, // 5分钟
		ActivityDecayRate:   0.95,
		PermanentThreshold:  3600.0, // 1小时
		DecisionInterval:    30 * time.Second,
		MonitoringInterval:  10 * time.Second,
	}
}

// ScalingDecision 伸缩决策
type ScalingDecision struct {
	NodeID      string    `json:"node_id"`
	Decision    bool      `json:"decision"`     // ui(t) 是否触发伸缩
	DecisionVar float64   `json:"decision_var"` // Δi(t) 决策变量
	Cost        float64   `json:"cost"`         // Cost_i(t,s)
	Timestamp   time.Time `json:"timestamp"`
	Reason      string    `json:"reason"`
}

// CloudProviderConfig 云服务商配置
type CloudProviderConfig struct {
	Provider       string            `json:"provider"` // AWS, VULTR, etc.
	Region         string            `json:"region"`
	Credentials    map[string]string `json:"credentials"` // API密钥等
	InstanceConfig InstanceConfig    `json:"instance_config"`
}

// InstanceConfig 实例配置
type InstanceConfig struct {
	InstanceType   string            `json:"instance_type"`
	ImageID        string            `json:"image_id"`
	KeyPair        string            `json:"key_pair"`
	SecurityGroups []string          `json:"security_groups"`
	UserData       string            `json:"user_data"`
	Tags           map[string]string `json:"tags"`
}

// ElasticControllerStats 弹性控制器统计
type ElasticControllerStats struct {
	TotalNodes     int `json:"total_nodes"`
	ActiveNodes    int `json:"active_nodes"`
	DormantNodes   int `json:"dormant_nodes"`
	ScalingUpNodes int `json:"scaling_up_nodes"`
	ReleasingNodes int `json:"releasing_nodes"`
	PermanentNodes int `json:"permanent_nodes"`

	TotalScalingDecisions int `json:"total_scaling_decisions"`
	SuccessfulScalings    int `json:"successful_scalings"`
	FailedScalings        int `json:"failed_scalings"`

	AverageVolatility   float64 `json:"average_volatility"`
	AveragePerturbation float64 `json:"average_perturbation"`
	TotalCost           float64 `json:"total_cost"`
}

// VolatilityMetrics 波动性指标
type VolatilityMetrics struct {
	NodeID          string    `json:"node_id"`
	VirtualQueue    float64   `json:"virtual_queue"`    // Õi(t) 当前虚拟队列值
	Perturbation    float64   `json:"perturbation"`     // Pi(t) 单时隙扰动
	VolatilityQueue float64   `json:"volatility_queue"` // Zi(t) 波动性队列
	PreviousVQ      float64   `json:"previous_vq"`      // 上一时隙的虚拟队列值
	Timestamp       time.Time `json:"timestamp"`

	// 计算参数
	CongestionThreshold float64 `json:"congestion_threshold"` // CV 拥塞阈值
	DecayFactor         float64 `json:"decay_factor"`         // γi 衰减因子
	SensitivityWeight   float64 `json:"sensitivity_weight"`   // w_i^V 敏感度权重
	StabilityCapacity   float64 `json:"stability_capacity"`   // Ci(t) 稳定能力
}

// BPScalerDecision BP-Scaler决策结果
type BPScalerDecision struct {
	NodeID       string           `json:"node_id"`
	ShouldScale  bool             `json:"should_scale"`  // ui(t) 是否触发伸缩
	DecisionVar  float64          `json:"decision_var"`  // Δi(t) 决策变量
	ScalingCost  float64          `json:"scaling_cost"`  // Cost_i(t,s) 伸缩成本
	CurrentState ElasticNodeState `json:"current_state"` // 当前状态
	TargetState  ElasticNodeState `json:"target_state"`  // 目标状态
	Timestamp    time.Time        `json:"timestamp"`
	Reason       string           `json:"reason"`

	// 计算详情（用于调试）
	VolatilityQueue float64 `json:"volatility_queue"` // Zi(t)
	Perturbation    float64 `json:"perturbation"`     // Pi(t)
	ScalingWeight   float64 `json:"scaling_weight"`   // w_i^S
	CostWeight      float64 `json:"cost_weight"`      // V_S
}
