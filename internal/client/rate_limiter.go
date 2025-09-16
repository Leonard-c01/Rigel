package client

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rigel/pkg/log"
)

// RateLimiter 速率限制器 - 实现令牌桶算法进行速率控制
type RateLimiter struct {
	// 令牌桶参数
	capacity   int64   // 桶容量
	tokens     int64   // 当前令牌数
	rate       float64 // 令牌生成速率 (tokens/second)
	lastRefill int64   // 上次填充时间 (nanoseconds)

	// 统计
	totalRequests   int64
	grantedRequests int64
	blockedRequests int64
	totalWaitTime   int64 // nanoseconds

	// 控制
	mu     sync.Mutex
	closed int32
}

// RateLimiterStats 速率限制器统计
type RateLimiterStats struct {
	TotalRequests   int64   `json:"total_requests"`
	GrantedRequests int64   `json:"granted_requests"`
	BlockedRequests int64   `json:"blocked_requests"`
	GrantRate       float64 `json:"grant_rate"`
	AverageWaitTime float64 `json:"average_wait_time_ms"`
	CurrentTokens   int64   `json:"current_tokens"`
	CurrentRate     float64 `json:"current_rate"`
}

// NewRateLimiter 创建速率限制器
func NewRateLimiter(burstCapacity int) *RateLimiter {
	now := time.Now().UnixNano()

	rl := &RateLimiter{
		capacity:   int64(burstCapacity),
		tokens:     int64(burstCapacity), // 初始时桶是满的
		rate:       1000.0,               // 默认1000 tokens/sec
		lastRefill: now,
	}

	log.Infof("Rate limiter created: capacity=%d, initial_rate=%.2f tokens/sec",
		burstCapacity, rl.rate)

	return rl
}

// WaitForToken 等待令牌（根据推荐速率调整）
func (rl *RateLimiter) WaitForToken(recommendedRate float64) error {
	if atomic.LoadInt32(&rl.closed) == 1 {
		return context.Canceled
	}

	atomic.AddInt64(&rl.totalRequests, 1)
	startTime := time.Now()

	// 更新速率
	rl.updateRate(recommendedRate)

	// 尝试获取令牌
	if rl.tryAcquireToken() {
		atomic.AddInt64(&rl.grantedRequests, 1)
		return nil
	}

	// 需要等待
	waitTime := rl.calculateWaitTime()
	if waitTime > 0 {
		log.Debugf("Rate limiting: waiting %.2fms for token (rate=%.2f)",
			float64(waitTime)/1e6, recommendedRate)

		timer := time.NewTimer(waitTime)
		defer timer.Stop()

		select {
		case <-timer.C:
			// 等待完成，再次尝试获取令牌
			if rl.tryAcquireToken() {
				atomic.AddInt64(&rl.grantedRequests, 1)
				waitDuration := time.Since(startTime).Nanoseconds()
				atomic.AddInt64(&rl.totalWaitTime, waitDuration)
				return nil
			}
		case <-context.Background().Done():
			atomic.AddInt64(&rl.blockedRequests, 1)
			return context.Canceled
		}
	}

	atomic.AddInt64(&rl.blockedRequests, 1)
	return nil
}

// updateRate 更新令牌生成速率
func (rl *RateLimiter) updateRate(recommendedRate float64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// 将字节速率转换为令牌速率
	// 假设每个令牌代表1KB的数据
	newRate := recommendedRate / 1024 // bytes/sec -> tokens/sec

	if newRate != rl.rate {
		log.Debugf("Rate limiter: updating rate %.2f -> %.2f tokens/sec",
			rl.rate, newRate)
		rl.rate = newRate
	}
}

// tryAcquireToken 尝试获取令牌
func (rl *RateLimiter) tryAcquireToken() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// 先填充令牌
	rl.refillTokens()

	// 检查是否有可用令牌
	if rl.tokens > 0 {
		rl.tokens--
		return true
	}

	return false
}

// refillTokens 填充令牌
func (rl *RateLimiter) refillTokens() {
	now := time.Now().UnixNano()
	elapsed := now - rl.lastRefill

	if elapsed <= 0 {
		return
	}

	// 计算应该添加的令牌数
	tokensToAdd := int64(float64(elapsed) * rl.rate / 1e9) // nanoseconds to seconds

	if tokensToAdd > 0 {
		rl.tokens += tokensToAdd
		if rl.tokens > rl.capacity {
			rl.tokens = rl.capacity
		}
		rl.lastRefill = now
	}
}

// calculateWaitTime 计算等待时间
func (rl *RateLimiter) calculateWaitTime() time.Duration {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if rl.rate <= 0 {
		return time.Second // 如果速率为0，等待1秒
	}

	// 计算生成一个令牌需要的时间
	timePerToken := time.Duration(1e9 / rl.rate) // nanoseconds

	return timePerToken
}

// SetRate 设置速率
func (rl *RateLimiter) SetRate(rate float64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if rate < 0 {
		rate = 0
	}

	log.Debugf("Rate limiter: setting rate to %.2f tokens/sec", rate)
	rl.rate = rate
}

// SetCapacity 设置容量
func (rl *RateLimiter) SetCapacity(capacity int64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if capacity < 1 {
		capacity = 1
	}

	log.Debugf("Rate limiter: setting capacity to %d tokens", capacity)
	rl.capacity = capacity

	// 如果当前令牌数超过新容量，调整到新容量
	if rl.tokens > capacity {
		rl.tokens = capacity
	}
}

// GetCurrentRate 获取当前速率
func (rl *RateLimiter) GetCurrentRate() float64 {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.rate
}

// GetAvailableTokens 获取可用令牌数
func (rl *RateLimiter) GetAvailableTokens() int64 {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.refillTokens()
	return rl.tokens
}

// GetStats 获取统计信息
func (rl *RateLimiter) GetStats() *RateLimiterStats {
	totalRequests := atomic.LoadInt64(&rl.totalRequests)
	grantedRequests := atomic.LoadInt64(&rl.grantedRequests)
	totalWaitTime := atomic.LoadInt64(&rl.totalWaitTime)

	var grantRate float64
	if totalRequests > 0 {
		grantRate = float64(grantedRequests) / float64(totalRequests)
	}

	var averageWaitTime float64
	if grantedRequests > 0 {
		averageWaitTime = float64(totalWaitTime) / float64(grantedRequests) / 1e6 // convert to ms
	}

	return &RateLimiterStats{
		TotalRequests:   totalRequests,
		GrantedRequests: grantedRequests,
		BlockedRequests: atomic.LoadInt64(&rl.blockedRequests),
		GrantRate:       grantRate,
		AverageWaitTime: averageWaitTime,
		CurrentTokens:   rl.GetAvailableTokens(),
		CurrentRate:     rl.GetCurrentRate(),
	}
}

// Reset 重置统计信息
func (rl *RateLimiter) Reset() {
	atomic.StoreInt64(&rl.totalRequests, 0)
	atomic.StoreInt64(&rl.grantedRequests, 0)
	atomic.StoreInt64(&rl.blockedRequests, 0)
	atomic.StoreInt64(&rl.totalWaitTime, 0)

	rl.mu.Lock()
	rl.tokens = rl.capacity
	rl.lastRefill = time.Now().UnixNano()
	rl.mu.Unlock()

	log.Info("Rate limiter reset")
}

// Close 关闭速率限制器
func (rl *RateLimiter) Close() error {
	atomic.StoreInt32(&rl.closed, 1)
	log.Info("Rate limiter closed")
	return nil
}

// AdaptiveRateLimiter 自适应速率限制器
type AdaptiveRateLimiter struct {
	*RateLimiter

	// 自适应参数
	targetLatency    time.Duration
	adjustmentFactor float64
	minRate          float64
	maxRate          float64

	// 监控
	recentLatencies []time.Duration
	latencyIndex    int
	mu              sync.Mutex
}

// NewAdaptiveRateLimiter 创建自适应速率限制器
func NewAdaptiveRateLimiter(burstCapacity int, targetLatency time.Duration) *AdaptiveRateLimiter {
	return &AdaptiveRateLimiter{
		RateLimiter:      NewRateLimiter(burstCapacity),
		targetLatency:    targetLatency,
		adjustmentFactor: 0.1,
		minRate:          10.0,
		maxRate:          10000.0,
		recentLatencies:  make([]time.Duration, 10),
	}
}

// AdjustRate 根据延迟调整速率
func (arl *AdaptiveRateLimiter) AdjustRate(latency time.Duration) {
	arl.mu.Lock()
	defer arl.mu.Unlock()

	// 记录延迟
	arl.recentLatencies[arl.latencyIndex] = latency
	arl.latencyIndex = (arl.latencyIndex + 1) % len(arl.recentLatencies)

	// 计算平均延迟
	var totalLatency time.Duration
	validSamples := 0
	for _, lat := range arl.recentLatencies {
		if lat > 0 {
			totalLatency += lat
			validSamples++
		}
	}

	if validSamples == 0 {
		return
	}

	avgLatency := totalLatency / time.Duration(validSamples)
	currentRate := arl.GetCurrentRate()

	// 根据延迟调整速率
	if avgLatency > arl.targetLatency {
		// 延迟过高，降低速率
		newRate := currentRate * (1 - arl.adjustmentFactor)
		if newRate < arl.minRate {
			newRate = arl.minRate
		}
		arl.SetRate(newRate)
		log.Debugf("Adaptive rate limiter: latency too high (%.2fms > %.2fms), reducing rate %.2f -> %.2f",
			float64(avgLatency)/1e6, float64(arl.targetLatency)/1e6, currentRate, newRate)
	} else if avgLatency < arl.targetLatency/2 {
		// 延迟很低，可以提高速率
		newRate := currentRate * (1 + arl.adjustmentFactor)
		if newRate > arl.maxRate {
			newRate = arl.maxRate
		}
		arl.SetRate(newRate)
		log.Debugf("Adaptive rate limiter: latency low (%.2fms < %.2fms), increasing rate %.2f -> %.2f",
			float64(avgLatency)/1e6, float64(arl.targetLatency/2)/1e6, currentRate, newRate)
	}
}
