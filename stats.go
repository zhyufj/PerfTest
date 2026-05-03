package main

import (
	"math"
	"sort"
	"sync"
	"time"
)

type SecondData struct {
	Second     int
	TPS        float64
	AvgLatency time.Duration
	Errors     int
	TotalReqs  int
}

type StatsCollector struct {
	mu             sync.Mutex
	durations      []time.Duration
	statusCodes    map[int]int64 // 状态码统计
	totalBytes     int64         // 总接收字节数
	successSamples []string      // 成功响应采样（最多5个）
	errorSamples   []string      // 失败响应采样（最多5个）
	history        []SecondData  // 每秒历史数据
}

func NewStatsCollector() *StatsCollector {
	return &StatsCollector{
		statusCodes:    make(map[int]int64),
		successSamples: make([]string, 0, 5),
		errorSamples:   make([]string, 0, 5),
	}
}

func (s *StatsCollector) Record(d time.Duration, statusCode int, responseBody string, bytes int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.statusCodes[statusCode]++
	s.totalBytes += int64(bytes)

	success := statusCode >= 200 && statusCode < 400
	if success {
		s.durations = append(s.durations, d)
		if len(s.successSamples) < 5 {
			s.successSamples = append(s.successSamples, responseBody)
		}
	} else {
		if len(s.errorSamples) < 5 {
			s.errorSamples = append(s.errorSamples, responseBody)
		}
	}
}

func (s *StatsCollector) GetStatusSummary() (status2xx, status3xx, status4xx, status5xx int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for code, count := range s.statusCodes {
		switch {
		case code >= 200 && code < 300:
			status2xx += count
		case code >= 300 && code < 400:
			status3xx += count
		case code >= 400 && code < 500:
			status4xx += count
		case code >= 500:
			status5xx += count
		}
	}
	return
}

func (s *StatsCollector) GetSamples() ([]string, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.successSamples, s.errorSamples
}

func (s *StatsCollector) Snapshot() (total int, errors int64, avg, p50, p90, p95, p99 time.Duration, min, max time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := len(s.durations)
	var totalRequests int64
	for _, cnt := range s.statusCodes {
		totalRequests += cnt
	}
	errors = totalRequests - int64(l)
	total = int(totalRequests)
	if l == 0 {
		return
	}
	sorted := make([]time.Duration, l)
	copy(sorted, s.durations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	min = sorted[0]
	max = sorted[l-1]
	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	avg = sum / time.Duration(l)
	p50 = percentile(sorted, 0.5)
	p90 = percentile(sorted, 0.9)
	p95 = percentile(sorted, 0.95)
	p99 = percentile(sorted, 0.99)
	return
}

func (s *StatsCollector) AddHistory(h SecondData) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.history = append(s.history, h)
}

func (s *StatsCollector) GetHistory() []SecondData {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.history
}

func (s *StatsCollector) GetTotalBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalBytes
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted))) - 1)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
