package client

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rigel/internal/protocol"
	"github.com/rigel/pkg/log"
)

// PathEncoder 路径编码器 - 负责将路径信息封装到数据块中
type PathEncoder struct {
	blockIDCounter uint64
}

// NewPathEncoder 创建路径编码器
func NewPathEncoder() *PathEncoder {
	return &PathEncoder{
		blockIDCounter: uint64(time.Now().UnixNano()),
	}
}

// EncodeWithPath 将数据和路径信息编码为数据块
func (pe *PathEncoder) EncodeWithPath(data []byte, path []string, priority uint8) (*protocol.DataBlock, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}

	if len(path) == 0 {
		return nil, fmt.Errorf("empty path")
	}

	// 生成唯一的块ID
	pe.blockIDCounter++
	blockID := pe.blockIDCounter

	// 构建路由信息
	routeInfo, err := pe.buildRouteInfo(path, priority)
	if err != nil {
		return nil, fmt.Errorf("failed to build route info: %v", err)
	}

	// 创建数据块
	dataBlock := protocol.CreateDataBlock(blockID, data, *routeInfo)

	log.Debugf("Encoded data block: ID=%d, size=%d, path=%v, priority=%d",
		blockID, len(data), path, priority)

	return dataBlock, nil
}

// buildRouteInfo 构建路由信息
func (pe *PathEncoder) buildRouteInfo(path []string, priority uint8) (*protocol.RouteInfo, error) {
	if len(path) == 0 {
		return nil, fmt.Errorf("empty path")
	}

	// 将路径编码为目标地址字符串
	// 格式: "hop1,hop2,hop3,target"
	pathString := strings.Join(path, ",")

	// 构建路由信息
	routeInfo := &protocol.RouteInfo{
		TargetAddress: pathString,
		Priority:      priority,
		TTL:           64, // 默认TTL
		Flags:         protocol.RouteFlagDirect,
		Timestamp:     time.Now().Unix(),
	}

	// 根据路径长度设置标志
	if len(path) > 1 {
		routeInfo.Flags |= protocol.RouteFlagMultiPath
	}

	return routeInfo, nil
}

// EncodeWithComplexPath 编码复杂路径信息
func (pe *PathEncoder) EncodeWithComplexPath(data []byte, complexPath *ComplexPath) (*protocol.DataBlock, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}

	if complexPath == nil {
		return nil, fmt.Errorf("nil complex path")
	}

	// 生成唯一的块ID
	pe.blockIDCounter++
	blockID := pe.blockIDCounter

	// 将复杂路径序列化为JSON
	pathJSON, err := json.Marshal(complexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal complex path: %v", err)
	}

	// 构建路由信息
	routeInfo := &protocol.RouteInfo{
		TargetAddress: string(pathJSON),
		Priority:      complexPath.Priority,
		TTL:           complexPath.TTL,
		Flags:         pe.buildFlags(complexPath),
		Timestamp:     time.Now().Unix(),
	}

	// 创建数据块
	dataBlock := protocol.CreateDataBlock(blockID, data, *routeInfo)

	log.Debugf("Encoded complex data block: ID=%d, size=%d, hops=%d, priority=%d",
		blockID, len(data), len(complexPath.Hops), complexPath.Priority)

	return dataBlock, nil
}

// buildFlags 构建标志位
func (pe *PathEncoder) buildFlags(complexPath *ComplexPath) uint16 {
	var flags uint16

	// 基础标志
	flags |= protocol.RouteFlagDirect

	// 多路径标志
	if len(complexPath.Hops) > 1 {
		flags |= protocol.RouteFlagMultiPath
	}

	// 加密标志
	if complexPath.RequireEncryption {
		flags |= protocol.RouteFlagEncrypted
	}

	// 压缩标志
	if complexPath.RequireCompression {
		flags |= protocol.RouteFlagCompressed
	}

	return flags
}

// DecodePathFromBlock 从数据块中解码路径信息
func (pe *PathEncoder) DecodePathFromBlock(dataBlock *protocol.DataBlock) (*DecodedPath, error) {
	if dataBlock == nil {
		return nil, fmt.Errorf("nil data block")
	}

	routeInfo := dataBlock.Header.RouteInfo

	// 尝试解析为简单路径
	if !strings.HasPrefix(routeInfo.TargetAddress, "{") {
		// 简单路径格式: "hop1,hop2,hop3"
		hops := strings.Split(routeInfo.TargetAddress, ",")
		return &DecodedPath{
			Type:     PathTypeSimple,
			Hops:     hops,
			Priority: routeInfo.Priority,
			TTL:      routeInfo.TTL,
			Flags:    routeInfo.Flags,
		}, nil
	}

	// 尝试解析为复杂路径
	var complexPath ComplexPath
	if err := json.Unmarshal([]byte(routeInfo.TargetAddress), &complexPath); err != nil {
		return nil, fmt.Errorf("failed to unmarshal complex path: %v", err)
	}

	return &DecodedPath{
		Type:        PathTypeComplex,
		ComplexPath: &complexPath,
		Priority:    routeInfo.Priority,
		TTL:         routeInfo.TTL,
		Flags:       routeInfo.Flags,
	}, nil
}

// GetNextHop 获取下一跳地址
func (pe *PathEncoder) GetNextHop(dataBlock *protocol.DataBlock, currentHop string) (string, error) {
	decodedPath, err := pe.DecodePathFromBlock(dataBlock)
	if err != nil {
		return "", fmt.Errorf("failed to decode path: %v", err)
	}

	switch decodedPath.Type {
	case PathTypeSimple:
		return pe.getNextHopFromSimplePath(decodedPath.Hops, currentHop)
	case PathTypeComplex:
		return pe.getNextHopFromComplexPath(decodedPath.ComplexPath, currentHop)
	default:
		return "", fmt.Errorf("unknown path type: %d", decodedPath.Type)
	}
}

// getNextHopFromSimplePath 从简单路径获取下一跳
func (pe *PathEncoder) getNextHopFromSimplePath(hops []string, currentHop string) (string, error) {
	if len(hops) == 0 {
		return "", fmt.Errorf("empty hops")
	}

	// 如果当前跳为空，返回第一跳
	if currentHop == "" {
		return hops[0], nil
	}

	// 查找当前跳在路径中的位置
	for i, hop := range hops {
		if hop == currentHop {
			// 如果是最后一跳，返回空（表示到达目标）
			if i == len(hops)-1 {
				return "", nil
			}
			// 返回下一跳
			return hops[i+1], nil
		}
	}

	return "", fmt.Errorf("current hop %s not found in path", currentHop)
}

// getNextHopFromComplexPath 从复杂路径获取下一跳
func (pe *PathEncoder) getNextHopFromComplexPath(complexPath *ComplexPath, currentHop string) (string, error) {
	if complexPath == nil || len(complexPath.Hops) == 0 {
		return "", fmt.Errorf("empty complex path")
	}

	// 如果当前跳为空，返回第一跳
	if currentHop == "" {
		return complexPath.Hops[0].Address, nil
	}

	// 查找当前跳
	for i, hop := range complexPath.Hops {
		if hop.Address == currentHop {
			// 如果是最后一跳，返回目标地址
			if i == len(complexPath.Hops)-1 {
				return complexPath.FinalTarget, nil
			}
			// 返回下一跳
			return complexPath.Hops[i+1].Address, nil
		}
	}

	return "", fmt.Errorf("current hop %s not found in complex path", currentHop)
}

// ComplexPath 复杂路径信息
type ComplexPath struct {
	Hops               []PathHop `json:"hops"`
	FinalTarget        string    `json:"final_target"`
	Priority           uint8     `json:"priority"`
	TTL                uint8     `json:"ttl"`
	RequireEncryption  bool      `json:"require_encryption"`
	RequireCompression bool      `json:"require_compression"`
	QoSRequirements    *QoS      `json:"qos_requirements,omitempty"`
	LoadBalancingHints []string  `json:"load_balancing_hints,omitempty"`
}

// PathHop 路径跳点
type PathHop struct {
	Address     string            `json:"address"`
	Weight      float64           `json:"weight"`
	Constraints map[string]string `json:"constraints,omitempty"`
}

// QoS 服务质量要求
type QoS struct {
	MaxLatency   int     `json:"max_latency_ms"`
	MinBandwidth int64   `json:"min_bandwidth_bps"`
	MaxLossRate  float64 `json:"max_loss_rate"`
}

// DecodedPath 解码后的路径
type DecodedPath struct {
	Type        PathType     `json:"type"`
	Hops        []string     `json:"hops,omitempty"`         // 简单路径
	ComplexPath *ComplexPath `json:"complex_path,omitempty"` // 复杂路径
	Priority    uint8        `json:"priority"`
	TTL         uint8        `json:"ttl"`
	Flags       uint16       `json:"flags"`
}

// PathType 路径类型
type PathType int

const (
	PathTypeSimple  PathType = iota // 简单路径
	PathTypeComplex                 // 复杂路径
)

func (pt PathType) String() string {
	switch pt {
	case PathTypeSimple:
		return "simple"
	case PathTypeComplex:
		return "complex"
	default:
		return "unknown"
	}
}
