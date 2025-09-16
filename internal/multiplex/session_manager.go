package multiplex

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/rigel/pkg/log"

	"github.com/xtaci/smux"
)

// TCPSessionManager TCP会话管理器实现
type TCPSessionManager struct {
	config   *MultiplexConfig
	sessions map[string]*MuxSession
	targets  map[string][]*MuxSession // target -> sessions
	mu       sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewTCPSessionManager 创建TCP会话管理器
func NewTCPSessionManager(config *MultiplexConfig) *TCPSessionManager {
	if config == nil {
		config = DefaultMultiplexConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	sm := &TCPSessionManager{
		config:   config,
		sessions: make(map[string]*MuxSession),
		targets:  make(map[string][]*MuxSession),
		ctx:      ctx,
		cancel:   cancel,
	}

	// 启动维护协程
	sm.wg.Add(1)
	go sm.maintenanceLoop()

	return sm
}

// CreateSession 创建新会话
func (sm *TCPSessionManager) CreateSession(ctx context.Context, target string) (*MuxSession, error) {
	log.Debugf("Creating new session to target: %s", target)

	// 检查会话数限制
	sm.mu.RLock()
	totalSessions := len(sm.sessions)
	sm.mu.RUnlock()

	if totalSessions >= sm.config.MaxSessions {
		return nil, fmt.Errorf("maximum sessions limit reached: %d", sm.config.MaxSessions)
	}

	// 建立TCP连接
	conn, err := net.DialTimeout("tcp", target, 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to dial target %s: %v", target, err)
	}

	// 创建smux配置
	smuxConfig := smux.DefaultConfig()
	smuxConfig.KeepAliveInterval = sm.config.KeepAliveInterval
	smuxConfig.KeepAliveTimeout = sm.config.KeepAliveTimeout
	smuxConfig.MaxFrameSize = sm.config.MaxFrameSize
	smuxConfig.MaxReceiveBuffer = sm.config.MaxReceiveBuffer
	smuxConfig.MaxStreamBuffer = sm.config.MaxStreamBuffer

	// 创建smux会话
	session, err := smux.Client(conn, smuxConfig)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create smux session: %v", err)
	}

	// 创建会话包装器
	sessionID := sm.generateSessionID()
	muxSession := &MuxSession{
		ID:        sessionID,
		Session:   session,
		Target:    target,
		Status:    SessionStatusActive,
		CreatedAt: time.Now(),
		LastUsed:  time.Now(),
	}

	// 添加到管理器
	sm.mu.Lock()
	sm.sessions[sessionID] = muxSession
	sm.targets[target] = append(sm.targets[target], muxSession)
	sm.mu.Unlock()

	log.Infof("Created new session %s to target %s", sessionID, target)
	return muxSession, nil
}

// GetSession 获取可用会话
func (sm *TCPSessionManager) GetSession(target string) (*MuxSession, error) {
	sm.mu.RLock()
	sessions := sm.targets[target]
	sm.mu.RUnlock()

	// 查找可用的会话
	for _, session := range sessions {
		if session.IsHealthy() && session.GetStreamCount() < int32(sm.config.MaxStreamsPerSession) {
			session.SetStatus(SessionStatusActive)
			return session, nil
		}
	}

	// 没有可用会话，创建新的
	return sm.CreateSession(sm.ctx, target)
}

// ReturnSession 归还会话
func (sm *TCPSessionManager) ReturnSession(session *MuxSession) {
	if session == nil {
		return
	}

	// 检查会话是否仍然健康
	if !session.IsHealthy() {
		sm.CloseSession(session.ID)
		return
	}

	// 如果没有活跃流，标记为空闲
	if session.GetStreamCount() == 0 {
		session.SetStatus(SessionStatusIdle)
	}

	log.Debugf("Returned session %s, stream count: %d", session.ID, session.GetStreamCount())
}

// CloseSession 关闭会话
func (sm *TCPSessionManager) CloseSession(sessionID string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	session, exists := sm.sessions[sessionID]
	if !exists {
		return fmt.Errorf("session %s not found", sessionID)
	}

	// 设置状态为关闭中
	session.SetStatus(SessionStatusClosing)

	// 关闭smux会话
	if session.Session != nil {
		session.Session.Close()
	}

	// 从管理器中移除
	delete(sm.sessions, sessionID)

	// 从目标映射中移除
	if targetSessions, exists := sm.targets[session.Target]; exists {
		for i, s := range targetSessions {
			if s.ID == sessionID {
				sm.targets[session.Target] = append(targetSessions[:i], targetSessions[i+1:]...)
				break
			}
		}
		// 如果目标没有会话了，删除映射
		if len(sm.targets[session.Target]) == 0 {
			delete(sm.targets, session.Target)
		}
	}

	session.SetStatus(SessionStatusClosed)
	log.Infof("Closed session %s to target %s", sessionID, session.Target)
	return nil
}

// GetStats 获取会话管理器统计
func (sm *TCPSessionManager) GetStats() *SessionManagerStats {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	stats := &SessionManagerStats{
		TotalSessions: len(sm.sessions),
	}

	for _, session := range sm.sessions {
		switch session.GetStatus() {
		case SessionStatusActive:
			stats.ActiveSessions++
		case SessionStatusIdle:
			stats.IdleSessions++
		}

		stats.TotalStreams += session.GetStreamCount()
		stats.TotalBytesSent += session.BytesSent
		stats.TotalBytesRecv += session.BytesRecv
	}

	return stats
}

// Close 关闭会话管理器
func (sm *TCPSessionManager) Close() error {
	log.Info("Closing session manager")

	sm.cancel()
	sm.wg.Wait()

	// 关闭所有会话
	sm.mu.RLock()
	sessionIDs := make([]string, 0, len(sm.sessions))
	for sessionID := range sm.sessions {
		sessionIDs = append(sessionIDs, sessionID)
	}
	sm.mu.RUnlock()

	for _, sessionID := range sessionIDs {
		sm.CloseSession(sessionID)
	}

	log.Info("Session manager closed")
	return nil
}

// generateSessionID 生成会话ID
func (sm *TCPSessionManager) generateSessionID() string {
	bytes := make([]byte, 8)
	rand.Read(bytes)
	return fmt.Sprintf("session_%x", bytes)
}

// maintenanceLoop 维护循环
func (sm *TCPSessionManager) maintenanceLoop() {
	defer sm.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.performMaintenance()
		}
	}
}

// performMaintenance 执行维护任务
func (sm *TCPSessionManager) performMaintenance() {
	sm.mu.RLock()
	sessionsToClose := make([]string, 0)
	now := time.Now()

	for sessionID, session := range sm.sessions {
		// 检查会话健康状态
		if !session.IsHealthy() {
			sessionsToClose = append(sessionsToClose, sessionID)
			continue
		}

		// 检查空闲超时
		if session.GetStatus() == SessionStatusIdle &&
			now.Sub(session.LastUsed) > sm.config.SessionIdleTimeout {
			sessionsToClose = append(sessionsToClose, sessionID)
			continue
		}

		// 检查会话超时
		if now.Sub(session.CreatedAt) > sm.config.SessionTimeout {
			sessionsToClose = append(sessionsToClose, sessionID)
			continue
		}
	}
	sm.mu.RUnlock()

	// 关闭需要清理的会话
	for _, sessionID := range sessionsToClose {
		sm.CloseSession(sessionID)
		log.Debugf("Closed session %s during maintenance", sessionID)
	}

	// 确保最小会话数
	sm.ensureMinSessions()

	// 记录统计信息
	stats := sm.GetStats()
	log.Debugf("Session manager stats: total=%d, active=%d, idle=%d, streams=%d",
		stats.TotalSessions, stats.ActiveSessions, stats.IdleSessions, stats.TotalStreams)
}

// ensureMinSessions 确保最小会话数
func (sm *TCPSessionManager) ensureMinSessions() {
	sm.mu.RLock()
	targets := make([]string, 0, len(sm.targets))
	for target := range sm.targets {
		targets = append(targets, target)
	}
	sm.mu.RUnlock()

	for _, target := range targets {
		sm.mu.RLock()
		sessionCount := len(sm.targets[target])
		sm.mu.RUnlock()

		if sessionCount < sm.config.MinSessions {
			needed := sm.config.MinSessions - sessionCount
			for i := 0; i < needed; i++ {
				_, err := sm.CreateSession(sm.ctx, target)
				if err != nil {
					log.Errorf("Failed to create minimum session for target %s: %v", target, err)
					break
				}
			}
		}
	}
}
