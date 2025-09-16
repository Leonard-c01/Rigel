package multiplex

import (
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"github.com/rigel/pkg/log"
)

// TCPStreamManager TCP流管理器实现
type TCPStreamManager struct {
	sessionManager SessionManager
	streams        map[string]*MuxStream
	mu             sync.RWMutex
}

// NewTCPStreamManager 创建TCP流管理器
func NewTCPStreamManager(sessionManager SessionManager) *TCPStreamManager {
	return &TCPStreamManager{
		sessionManager: sessionManager,
		streams:        make(map[string]*MuxStream),
	}
}

// OpenStream 打开新流
func (sm *TCPStreamManager) OpenStream(sessionID string) (*MuxStream, error) {
	log.Debugf("Opening new stream for session: %s", sessionID)

	// 获取会话统计，找到对应的会话
	// 这里简化实现，实际应该从会话管理器获取具体会话
	stats := sm.sessionManager.GetStats()
	if stats.TotalSessions == 0 {
		return nil, fmt.Errorf("no available sessions")
	}

	// 创建流ID
	streamID := sm.generateStreamID()

	// 创建流包装器（这里简化，实际需要从具体会话创建流）
	muxStream := &MuxStream{
		ID:        streamID,
		Stream:    nil, // 实际实现中需要从smux会话创建
		SessionID: sessionID,
		CreatedAt: time.Now(),
		LastUsed:  time.Now(),
	}

	// 添加到管理器
	sm.mu.Lock()
	sm.streams[streamID] = muxStream
	sm.mu.Unlock()

	log.Debugf("Created new stream %s for session %s", streamID, sessionID)
	return muxStream, nil
}

// CloseStream 关闭流
func (sm *TCPStreamManager) CloseStream(streamID string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	stream, exists := sm.streams[streamID]
	if !exists {
		return fmt.Errorf("stream %s not found", streamID)
	}

	// 关闭smux流
	if stream.Stream != nil {
		stream.Stream.Close()
	}

	// 从管理器中移除
	delete(sm.streams, streamID)

	log.Debugf("Closed stream %s", streamID)
	return nil
}

// GetStream 获取流
func (sm *TCPStreamManager) GetStream(streamID string) (*MuxStream, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	stream, exists := sm.streams[streamID]
	if !exists {
		return nil, fmt.Errorf("stream %s not found", streamID)
	}

	return stream, nil
}

// GetStats 获取流管理器统计
func (sm *TCPStreamManager) GetStats() *StreamManagerStats {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	stats := &StreamManagerStats{
		TotalStreams:  len(sm.streams),
		ActiveStreams: 0,
	}

	for _, stream := range sm.streams {
		if stream.Stream != nil {
			// 简化检查：假设非nil的流都是活跃的
			stats.ActiveStreams++
		}
		stats.TotalBytesSent += stream.BytesSent
		stats.TotalBytesRecv += stream.BytesRecv
	}

	return stats
}

// generateStreamID 生成流ID
func (sm *TCPStreamManager) generateStreamID() string {
	bytes := make([]byte, 8)
	rand.Read(bytes)
	return fmt.Sprintf("stream_%x", bytes)
}

// ListStreams 列出所有流
func (sm *TCPStreamManager) ListStreams() []*MuxStream {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	streams := make([]*MuxStream, 0, len(sm.streams))
	for _, stream := range sm.streams {
		streams = append(streams, stream)
	}

	return streams
}

// GetStreamsBySession 获取指定会话的所有流
func (sm *TCPStreamManager) GetStreamsBySession(sessionID string) []*MuxStream {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	streams := make([]*MuxStream, 0)
	for _, stream := range sm.streams {
		if stream.SessionID == sessionID {
			streams = append(streams, stream)
		}
	}

	return streams
}

// CleanupClosedStreams 清理已关闭的流
func (sm *TCPStreamManager) CleanupClosedStreams() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	closedCount := 0
	streamsToDelete := make([]string, 0)

	for streamID, stream := range sm.streams {
		if stream.Stream == nil {
			// 流为nil，标记为需要删除
			streamsToDelete = append(streamsToDelete, streamID)
		}
	}

	for _, streamID := range streamsToDelete {
		delete(sm.streams, streamID)
		closedCount++
	}

	if closedCount > 0 {
		log.Debugf("Cleaned up %d closed streams", closedCount)
	}

	return closedCount
}

// GetStreamCount 获取流总数
func (sm *TCPStreamManager) GetStreamCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.streams)
}

// GetActiveStreamCount 获取活跃流数量
func (sm *TCPStreamManager) GetActiveStreamCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	activeCount := 0
	for _, stream := range sm.streams {
		if stream.Stream != nil {
			// 简化检查：假设非nil的流都是活跃的
			activeCount++
		}
	}

	return activeCount
}

// UpdateStreamStats 更新流统计信息
func (sm *TCPStreamManager) UpdateStreamStats(streamID string, sent, recv int64) error {
	sm.mu.RLock()
	stream, exists := sm.streams[streamID]
	sm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("stream %s not found", streamID)
	}

	stream.UpdateStats(sent, recv)
	return nil
}

// GetStreamStats 获取指定流的统计信息
func (sm *TCPStreamManager) GetStreamStats(streamID string) (int64, int64, error) {
	sm.mu.RLock()
	stream, exists := sm.streams[streamID]
	sm.mu.RUnlock()

	if !exists {
		return 0, 0, fmt.Errorf("stream %s not found", streamID)
	}

	sent, recv := stream.GetStats()
	return sent, recv, nil
}

// Close 关闭流管理器
func (sm *TCPStreamManager) Close() error {
	log.Info("Closing stream manager")

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 关闭所有流
	for streamID, stream := range sm.streams {
		if stream.Stream != nil {
			stream.Stream.Close()
		}
		delete(sm.streams, streamID)
	}

	log.Info("Stream manager closed")
	return nil
}
