package config

import "time"

// Config 动态代理配置
type Config struct {
	Common       CommonConfig       `json:"common" yaml:"common"`
	Listeners    []ListenerConfig   `json:"listeners" yaml:"listeners"`
	Scheduler    SchedulerConfig    `json:"scheduler" yaml:"scheduler"`
	Peers        []PeerConfig       `json:"peers" yaml:"peers"`
	ControlPlane ControlPlaneConfig `json:"control_plane" yaml:"control_plane"`
}

// CommonConfig 通用配置
type CommonConfig struct {
	LogLevel  string `json:"log_level" yaml:"log_level"`
	LogFile   string `json:"log_file" yaml:"log_file"`
	AdminAddr string `json:"admin_addr" yaml:"admin_addr"`
	NodeID    string `json:"node_id" yaml:"node_id"`
	Region    string `json:"region" yaml:"region"`
}

// ListenerConfig 监听器配置
type ListenerConfig struct {
	Name string `json:"name" yaml:"name"`
	Bind string `json:"bind" yaml:"bind"`
}

// SchedulerConfig 调度器配置
type SchedulerConfig struct {
	Address           string        `json:"address" yaml:"address"`
	ConnectTimeout    time.Duration `json:"connect_timeout" yaml:"connect_timeout"`
	RequestTimeout    time.Duration `json:"request_timeout" yaml:"request_timeout"`
	RetryInterval     time.Duration `json:"retry_interval" yaml:"retry_interval"`
	MaxRetries        int           `json:"max_retries" yaml:"max_retries"`
	HeartbeatInterval time.Duration `json:"heartbeat_interval" yaml:"heartbeat_interval"`
}

// PeerConfig 对等节点配置
type PeerConfig struct {
	Name    string `json:"name" yaml:"name"`
	Address string `json:"address" yaml:"address"`
	Type    string `json:"type,omitempty" yaml:"type,omitempty"` // tcp, tls等
	Region  string `json:"region,omitempty" yaml:"region,omitempty"`
}

// ControlPlaneConfig 控制平面配置
type ControlPlaneConfig struct {
	// 节点配置
	IsLeader bool   `json:"is_leader" yaml:"is_leader"`
	Address  string `json:"address" yaml:"address"`

	// 组长节点配置
	LeaderAddress string   `json:"leader_address" yaml:"leader_address"`
	LeaderNodes   []string `json:"leader_nodes" yaml:"leader_nodes"` // 其他区域的组长节点地址

	// 同步配置
	StatusReportInterval time.Duration `json:"status_report_interval" yaml:"status_report_interval"` // 状态上报间隔 (默认5秒)
	GlobalSyncInterval   time.Duration `json:"global_sync_interval" yaml:"global_sync_interval"`     // 全局同步间隔 (默认1秒)
	LeaderSyncInterval   time.Duration `json:"leader_sync_interval" yaml:"leader_sync_interval"`     // 组长间同步间隔 (默认1秒)

	// 超时配置
	HeartbeatTimeout time.Duration `json:"heartbeat_timeout" yaml:"heartbeat_timeout"` // 心跳超时 (默认30秒)
	RequestTimeout   time.Duration `json:"request_timeout" yaml:"request_timeout"`     // 请求超时 (默认10秒)

	// API配置
	APIPort string `json:"api_port" yaml:"api_port"` // API服务端口 (默认:9090)
}
