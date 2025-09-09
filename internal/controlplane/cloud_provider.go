package controlplane

import (
	"context"
	"fmt"
	"time"
)

// CloudProvider 云服务商接口
type CloudProvider interface {
	// CreateInstance 创建VM实例
	CreateInstance(ctx context.Context, config *InstanceConfig) (*CloudInstance, error)

	// DeleteInstance 删除VM实例
	DeleteInstance(ctx context.Context, instanceID string) error

	// GetInstance 获取实例信息
	GetInstance(ctx context.Context, instanceID string) (*CloudInstance, error)

	// ListInstances 列出所有实例
	ListInstances(ctx context.Context) ([]*CloudInstance, error)

	// GetProviderName 获取云服务商名称
	GetProviderName() string

	// ValidateConfig 验证配置
	ValidateConfig() error
}

// CloudInstance 云实例信息
type CloudInstance struct {
	InstanceID   string            `json:"instance_id"`
	Provider     string            `json:"provider"`
	Region       string            `json:"region"`
	InstanceType string            `json:"instance_type"`
	State        InstanceState     `json:"state"`
	PublicIP     string            `json:"public_ip"`
	PrivateIP    string            `json:"private_ip"`
	CreatedAt    time.Time         `json:"created_at"`
	Tags         map[string]string `json:"tags"`

	// Rigel特定信息
	NodeID    string `json:"node_id"`
	RigelPort string `json:"rigel_port"`
	IsReady   bool   `json:"is_ready"`
}

// InstanceState 实例状态
type InstanceState string

const (
	InstanceStatePending    InstanceState = "pending"
	InstanceStateRunning    InstanceState = "running"
	InstanceStateStopping   InstanceState = "stopping"
	InstanceStateStopped    InstanceState = "stopped"
	InstanceStateTerminated InstanceState = "terminated"
	InstanceStateError      InstanceState = "error"
)

// CloudProviderFactory 云服务商工厂
type CloudProviderFactory struct {
	providers map[string]func(*CloudProviderConfig) (CloudProvider, error)
}

// NewCloudProviderFactory 创建云服务商工厂
func NewCloudProviderFactory() *CloudProviderFactory {
	factory := &CloudProviderFactory{
		providers: make(map[string]func(*CloudProviderConfig) (CloudProvider, error)),
	}

	// 注册支持的云服务商
	factory.RegisterProvider("mock", NewMockCloudProvider)
	factory.RegisterProvider("aws", NewAWSCloudProvider)
	factory.RegisterProvider("vultr", NewVultrCloudProvider)

	return factory
}

// RegisterProvider 注册云服务商
func (f *CloudProviderFactory) RegisterProvider(name string, constructor func(*CloudProviderConfig) (CloudProvider, error)) {
	f.providers[name] = constructor
}

// CreateProvider 创建云服务商实例
func (f *CloudProviderFactory) CreateProvider(config *CloudProviderConfig) (CloudProvider, error) {
	constructor, exists := f.providers[config.Provider]
	if !exists {
		return nil, fmt.Errorf("unsupported cloud provider: %s", config.Provider)
	}

	return constructor(config)
}

// GetSupportedProviders 获取支持的云服务商列表
func (f *CloudProviderFactory) GetSupportedProviders() []string {
	providers := make([]string, 0, len(f.providers))
	for name := range f.providers {
		providers = append(providers, name)
	}
	return providers
}

// MockCloudProvider 模拟云服务商（用于测试）
type MockCloudProvider struct {
	config    *CloudProviderConfig
	instances map[string]*CloudInstance
}

// NewMockCloudProvider 创建模拟云服务商
func NewMockCloudProvider(config *CloudProviderConfig) (CloudProvider, error) {
	return &MockCloudProvider{
		config:    config,
		instances: make(map[string]*CloudInstance),
	}, nil
}

// CreateInstance 创建实例（模拟）
func (m *MockCloudProvider) CreateInstance(ctx context.Context, config *InstanceConfig) (*CloudInstance, error) {
	instanceID := fmt.Sprintf("mock-%d", time.Now().Unix())

	instance := &CloudInstance{
		InstanceID:   instanceID,
		Provider:     "mock",
		Region:       m.config.Region,
		InstanceType: config.InstanceType,
		State:        InstanceStateRunning,
		PublicIP:     fmt.Sprintf("192.168.1.%d", len(m.instances)+1),
		PrivateIP:    fmt.Sprintf("10.0.0.%d", len(m.instances)+1),
		CreatedAt:    time.Now(),
		Tags:         config.Tags,
		NodeID:       config.Tags["NodeID"],
		RigelPort:    "9090",
		IsReady:      true,
	}

	m.instances[instanceID] = instance
	return instance, nil
}

// DeleteInstance 删除实例（模拟）
func (m *MockCloudProvider) DeleteInstance(ctx context.Context, instanceID string) error {
	instance, exists := m.instances[instanceID]
	if !exists {
		return fmt.Errorf("instance %s not found", instanceID)
	}

	instance.State = InstanceStateTerminated
	delete(m.instances, instanceID)
	return nil
}

// GetInstance 获取实例信息（模拟）
func (m *MockCloudProvider) GetInstance(ctx context.Context, instanceID string) (*CloudInstance, error) {
	instance, exists := m.instances[instanceID]
	if !exists {
		return nil, fmt.Errorf("instance %s not found", instanceID)
	}

	// 返回副本
	instanceCopy := *instance
	return &instanceCopy, nil
}

// ListInstances 列出所有实例（模拟）
func (m *MockCloudProvider) ListInstances(ctx context.Context) ([]*CloudInstance, error) {
	instances := make([]*CloudInstance, 0, len(m.instances))
	for _, instance := range m.instances {
		instanceCopy := *instance
		instances = append(instances, &instanceCopy)
	}
	return instances, nil
}

// GetProviderName 获取提供商名称
func (m *MockCloudProvider) GetProviderName() string {
	return "mock"
}

// ValidateConfig 验证配置
func (m *MockCloudProvider) ValidateConfig() error {
	if m.config == nil {
		return fmt.Errorf("cloud provider config cannot be nil")
	}
	return nil
}

// AWSCloudProvider AWS云服务商（占位符实现）
type AWSCloudProvider struct {
	config *CloudProviderConfig
}

// NewAWSCloudProvider 创建AWS云服务商
func NewAWSCloudProvider(config *CloudProviderConfig) (CloudProvider, error) {
	// TODO: 实现AWS SDK集成
	return &AWSCloudProvider{config: config}, nil
}

// CreateInstance AWS创建实例
func (a *AWSCloudProvider) CreateInstance(ctx context.Context, config *InstanceConfig) (*CloudInstance, error) {
	// TODO: 实现AWS EC2实例创建
	return nil, fmt.Errorf("AWS provider not implemented yet")
}

// DeleteInstance AWS删除实例
func (a *AWSCloudProvider) DeleteInstance(ctx context.Context, instanceID string) error {
	// TODO: 实现AWS EC2实例删除
	return fmt.Errorf("AWS provider not implemented yet")
}

// GetInstance AWS获取实例
func (a *AWSCloudProvider) GetInstance(ctx context.Context, instanceID string) (*CloudInstance, error) {
	// TODO: 实现AWS EC2实例查询
	return nil, fmt.Errorf("AWS provider not implemented yet")
}

// ListInstances AWS列出实例
func (a *AWSCloudProvider) ListInstances(ctx context.Context) ([]*CloudInstance, error) {
	// TODO: 实现AWS EC2实例列表
	return nil, fmt.Errorf("AWS provider not implemented yet")
}

// GetProviderName AWS提供商名称
func (a *AWSCloudProvider) GetProviderName() string {
	return "aws"
}

// ValidateConfig AWS验证配置
func (a *AWSCloudProvider) ValidateConfig() error {
	if a.config == nil {
		return fmt.Errorf("AWS config cannot be nil")
	}
	// TODO: 验证AWS特定配置
	return nil
}

// VultrCloudProvider Vultr云服务商（占位符实现）
type VultrCloudProvider struct {
	config *CloudProviderConfig
}

// NewVultrCloudProvider 创建Vultr云服务商
func NewVultrCloudProvider(config *CloudProviderConfig) (CloudProvider, error) {
	// TODO: 实现Vultr API集成
	return &VultrCloudProvider{config: config}, nil
}

// CreateInstance Vultr创建实例
func (v *VultrCloudProvider) CreateInstance(ctx context.Context, config *InstanceConfig) (*CloudInstance, error) {
	// TODO: 实现Vultr实例创建
	return nil, fmt.Errorf("Vultr provider not implemented yet")
}

// DeleteInstance Vultr删除实例
func (v *VultrCloudProvider) DeleteInstance(ctx context.Context, instanceID string) error {
	// TODO: 实现Vultr实例删除
	return fmt.Errorf("Vultr provider not implemented yet")
}

// GetInstance Vultr获取实例
func (v *VultrCloudProvider) GetInstance(ctx context.Context, instanceID string) (*CloudInstance, error) {
	// TODO: 实现Vultr实例查询
	return nil, fmt.Errorf("Vultr provider not implemented yet")
}

// ListInstances Vultr列出实例
func (v *VultrCloudProvider) ListInstances(ctx context.Context) ([]*CloudInstance, error) {
	// TODO: 实现Vultr实例列表
	return nil, fmt.Errorf("Vultr provider not implemented yet")
}

// GetProviderName Vultr提供商名称
func (v *VultrCloudProvider) GetProviderName() string {
	return "vultr"
}

// ValidateConfig Vultr验证配置
func (v *VultrCloudProvider) ValidateConfig() error {
	if v.config == nil {
		return fmt.Errorf("Vultr config cannot be nil")
	}
	// TODO: 验证Vultr特定配置
	return nil
}

// CloudManager 云服务管理器
type CloudManager struct {
	providers map[string]CloudProvider
	factory   *CloudProviderFactory
}

// NewCloudManager 创建云服务管理器
func NewCloudManager() *CloudManager {
	return &CloudManager{
		providers: make(map[string]CloudProvider),
		factory:   NewCloudProviderFactory(),
	}
}

// AddProvider 添加云服务商
func (cm *CloudManager) AddProvider(name string, config *CloudProviderConfig) error {
	provider, err := cm.factory.CreateProvider(config)
	if err != nil {
		return fmt.Errorf("failed to create provider %s: %w", name, err)
	}

	if err := provider.ValidateConfig(); err != nil {
		return fmt.Errorf("invalid config for provider %s: %w", name, err)
	}

	cm.providers[name] = provider
	return nil
}

// GetProvider 获取云服务商
func (cm *CloudManager) GetProvider(name string) (CloudProvider, error) {
	provider, exists := cm.providers[name]
	if !exists {
		return nil, fmt.Errorf("provider %s not found", name)
	}
	return provider, nil
}

// CreateInstance 在指定云服务商上创建实例
func (cm *CloudManager) CreateInstance(ctx context.Context, providerName string, config *InstanceConfig) (*CloudInstance, error) {
	provider, err := cm.GetProvider(providerName)
	if err != nil {
		return nil, err
	}

	return provider.CreateInstance(ctx, config)
}

// DeleteInstance 删除指定云服务商上的实例
func (cm *CloudManager) DeleteInstance(ctx context.Context, providerName, instanceID string) error {
	provider, err := cm.GetProvider(providerName)
	if err != nil {
		return err
	}

	return provider.DeleteInstance(ctx, instanceID)
}

// GetAllInstances 获取所有云服务商的实例
func (cm *CloudManager) GetAllInstances(ctx context.Context) (map[string][]*CloudInstance, error) {
	result := make(map[string][]*CloudInstance)

	for name, provider := range cm.providers {
		instances, err := provider.ListInstances(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list instances from %s: %w", name, err)
		}
		result[name] = instances
	}

	return result, nil
}

// GetProviderNames 获取所有已配置的云服务商名称
func (cm *CloudManager) GetProviderNames() []string {
	names := make([]string, 0, len(cm.providers))
	for name := range cm.providers {
		names = append(names, name)
	}
	return names
}

// RemoveProvider 移除云服务商
func (cm *CloudManager) RemoveProvider(name string) error {
	if _, exists := cm.providers[name]; !exists {
		return fmt.Errorf("provider %s not found", name)
	}

	delete(cm.providers, name)
	return nil
}
