package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------- HTTP 状态码说明 ----------

var statusCodeText = map[int]string{
	200: "OK 请求成功",
	201: "Created 创建成功",
	204: "No Content 无内容",
	301: "Moved Permanently 永久重定向",
	302: "Found 临时重定向",
	304: "Not Modified 缓存命中",
	400: "Bad Request 请求参数错误",
	401: "Unauthorized 未授权",
	403: "Forbidden 禁止访问",
	404: "Not Found 资源不存在",
	405: "Method Not Allowed 方法不允许",
	408: "Request Timeout 请求超时",
	409: "Conflict 资源冲突",
	413: "Payload Too Large 请求体过大",
	415: "Unsupported Media Type 媒体类型不支持",
	429: "Too Many Requests 请求过于频繁",
	500: "Internal Server Error 服务器内部错误",
	502: "Bad Gateway 网关错误",
	503: "Service Unavailable 服务不可用",
	504: "Gateway Timeout 网关超时",
	599: "Network Error 网络连接错误",
}

func getStatusCodeText(code int) string {
	if text, ok := statusCodeText[code]; ok {
		return text
	}
	return "未知状态码"
}

// ---------- 配置结构 ----------

type Plan struct {
	Name              string            `json:"name"`
	URL               string            `json:"url"`
	Method            string            `json:"method"`
	Headers           map[string]string `json:"headers,omitempty"`
	Body              string            `json:"body,omitempty"`
	Threads           int               `json:"threads"`
	RampUp            int               `json:"rampup"`             // 爬坡时间(秒)
	Duration          string            `json:"duration"`           // e.g. "30s", "1m"
	Report            string            `json:"report,omitempty"`   // html, json
	DisableKeepAlives bool              `json:"disable_keepalives"` // 禁用连接复用
}

// ---------- 统计收集器 ----------

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
			s.errorSamples = append(s.errorSamples, fmt.Sprintf("[%d] %s", statusCode, responseBody))
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
	// 拷贝一份排序，避免影响后续并发写
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

// ---------- 压测引擎 ----------

func runPlan(plan Plan) {
	// 解析持续时间
	dur, err := time.ParseDuration(plan.Duration)
	if err != nil {
		fmt.Printf("无效的 duration: %s\n", plan.Duration)
		return
	}

	// 优化连接池：Go默认只有2个并发连接 per host！
	transport := &http.Transport{
		MaxIdleConns:        plan.Threads * 5,
		MaxIdleConnsPerHost: plan.Threads * 2,
		MaxConnsPerHost:     plan.Threads * 2,
		IdleConnTimeout:     60 * time.Second,
		DisableCompression:  false,
		DisableKeepAlives:   plan.DisableKeepAlives,
		ForceAttemptHTTP2:   true,
	}

	if plan.DisableKeepAlives {
		fmt.Println("🔌  连接模式: 禁用连接池，每请求新建TCP连接（模拟浏览器行为）")
	} else {
		fmt.Printf("🔌  连接模式: 连接池已启用，共 %d 个连接槽\n", plan.Threads*2)
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse // 禁止自动重定向，压测结果更准确
		},
	}

	stats := &StatsCollector{
		statusCodes:    make(map[int]int64),
		successSamples: make([]string, 0, 5),
		errorSamples:   make([]string, 0, 5),
	}
	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	// 实时统计协程
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		start := time.Now()
		lastTotal := 0
		lastTime := start
		for {
			select {
			case <-ticker.C:
				total, errs, avg, _, _, _, _, _, _ := stats.Snapshot()
				s2xx, s3xx, s4xx, s5xx := stats.GetStatusSummary()

				// 计算瞬时吞吐量
				deltaReq := total - lastTotal
				deltaTime := time.Since(lastTime).Seconds()
				tps := float64(deltaReq) / deltaTime
				errRate := float64(errs) * 100 / float64(total)
				if total == 0 {
					errRate = 0
				}
				second := int(time.Since(start).Seconds()) + 1

				// 记录历史数据
				stats.mu.Lock()
				stats.history = append(stats.history, SecondData{
					Second:     second,
					TPS:        tps,
					AvgLatency: avg,
					Errors:     int(errs),
					TotalReqs:  int(total),
				})
				stats.mu.Unlock()

				fmt.Printf("[%ds] 总量:%-5d | 每秒:%-4.0f | 平均:%-8s | 成功:%-4d 2xx:%-4d 3xx:%-4d 4xx:%-4d 5xx:%-4d | 错误率:%.1f%%\n",
					second, total, tps, avg.Round(time.Microsecond),
					total-int(errs), s2xx, s3xx, s4xx, s5xx, errRate)

				lastTotal = total
				lastTime = time.Now()
			case <-stopChan:
				return
			}
		}
	}()

	// 监听提前终止按键
	userStop := make(chan struct{})
	go func() {
		fmt.Println("💡 提示: 随时输入 q 然后回车可以提前终止测试，并保存当前数据")
		reader := bufio.NewReader(os.Stdin)
		for {
			input, _ := reader.ReadString('\n')
			if strings.TrimSpace(input) == "q" {
				fmt.Println("\n⏹️  用户请求提前终止，正在保存数据...")
				close(userStop)
				break
			}
		}
	}()

	// 启动并发 worker
	startTime := time.Now()
	rampInterval := time.Duration(0)
	if plan.RampUp > 0 && plan.Threads > 0 {
		rampInterval = time.Duration(plan.RampUp*1000/plan.Threads) * time.Millisecond
		fmt.Printf("🎚️  爬坡模式: 每 %v 启动 1 线程\n", rampInterval)
	}

	for i := 0; i < plan.Threads; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			// 爬坡：每个线程等待对应时间后才开始工作
			if rampInterval > 0 {
				select {
				case <-time.After(time.Duration(workerID) * rampInterval):
				case <-userStop:
					return
				}
			}
			for {
				select {
				case <-userStop:
					return
				default:
					if time.Since(startTime) >= dur {
						return
					}
				}
				// 构造请求
				req, err := http.NewRequest(plan.Method, plan.URL, strings.NewReader(plan.Body))
				if err != nil {
					stats.Record(0, 400, err.Error(), 0)
					continue
				}
				for k, v := range plan.Headers {
					req.Header.Set(k, v)
				}
				reqStart := time.Now()
				resp, err := client.Do(req)
				latency := time.Since(reqStart)

				if err != nil {
					stats.Record(latency, 599, err.Error(), 0)
					continue
				}

				bodyBytes, _ := io.ReadAll(resp.Body)
				resp.Body.Close()

				bodyStr := string(bodyBytes)
				if len(bodyStr) > 500 {
					bodyStr = bodyStr[:500] + "..."
				}
				stats.Record(latency, resp.StatusCode, bodyStr, len(bodyBytes))
			}
		}(i)
	}

	wg.Wait()
	close(stopChan)

	// 最终汇总
	total, errs, avg, p50, p90, p95, p99, min, max := stats.Snapshot()
	successSamples, errorSamples := stats.GetSamples()
	success := total - int(errs)
	throughput := float64(total) / dur.Seconds()
	concurrencyEfficiency := throughput / float64(plan.Threads)

	fmt.Println("\n")
	fmt.Println("╔═════════════════════════════════════════════════════════╗")
	fmt.Println("║                    测试结果汇总                        ║")
	fmt.Println("╠═════════════════════════════════════════════════════════╣")
	if plan.DisableKeepAlives {
		fmt.Printf("║  📋 测试配置:  %-3d 线程 × %-6s | 无连接池 ⚠️         ║\n", plan.Threads, dur.Round(time.Second).String())
	} else if plan.RampUp > 0 {
		fmt.Printf("║  📋 测试配置:  %-3d 线程 × %-6s | 爬坡: %ds           ║\n", plan.Threads, dur.Round(time.Second).String(), plan.RampUp)
	} else {
		fmt.Printf("║  📋 测试配置:  %-3d 线程 × %-6s | 连接池 ✓            ║\n", plan.Threads, dur.Round(time.Second).String())
	}
	fmt.Printf("║  ✅ 总请求:    %-8d  |  成功: %-6d  |  失败: %-6d  ║\n", total, success, int(errs))
	fmt.Printf("║  📊 成功率:    %-30.2f%%  ║\n", float64(success)*100/float64(total))
	fmt.Printf("║  ⚡ 吞吐量:    %-18.2f req/s  |  效率: %.1f req/s/线程   ║\n", throughput, concurrencyEfficiency)
	fmt.Printf("║  📦 总数据:    %-18.2f MB                           ║\n", float64(stats.totalBytes)/1024/1024)
	fmt.Println("║                HTTP 状态码明细                                 ║")
	fmt.Println("╠════════════╤══════════╤══════════════════════════════════════╣")
	fmt.Println("║   状态码   │   数量   │              说明                  ║")
	fmt.Println("╟────────────┼──────────┼──────────────────────────────────────╢")
	stats.mu.Lock()
	// 按状态码大小排序输出
	codes := make([]int, 0, len(stats.statusCodes))
	for code := range stats.statusCodes {
		codes = append(codes, code)
	}
	sort.Ints(codes)
	for _, code := range codes {
		cnt := stats.statusCodes[code]
		text := getStatusCodeText(code)
		switch {
		case code == 200:
			fmt.Printf("║  ✅  %3d   │  %6d  │  %-34s  ║\n", code, cnt, text)
		case code >= 200 && code < 300:
			fmt.Printf("║  ✅  %3d   │  %6d  │  %-34s  ║\n", code, cnt, text)
		case code >= 300 && code < 400:
			fmt.Printf("║  ⚠️   %3d   │  %6d  │  %-34s  ║\n", code, cnt, text)
		case code >= 400 && code < 500:
			fmt.Printf("║  ❌  %3d   │  %6d  │  %-34s  ║\n", code, cnt, text)
		case code >= 500:
			fmt.Printf("║  💥  %3d   │  %6d  │  %-34s  ║\n", code, cnt, text)
		}
	}
	stats.mu.Unlock()
	fmt.Println("╠════════════╧══════════╧══════════════════════════════════════╣")
	if success > 0 {
		fmt.Println("║                响应时间分布                                     ║")
		fmt.Println("╠═════════════════════════════════════════════════════════════════╣")
		fmt.Printf("║  ⏱️  平均:  %-12s  │  P50:  %-12s  │  P90:  %-12s    ║\n",
			avg.Round(time.Microsecond), p50.Round(time.Microsecond), p90.Round(time.Microsecond))
		fmt.Printf("║  ⏱️  P95:   %-12s  │  P99:  %-12s  │  最小:  %-12s    ║\n",
			p95.Round(time.Microsecond), p99.Round(time.Microsecond), min.Round(time.Microsecond))
		fmt.Printf("║  ⏱️  最大:  %-54s    ║\n", max.Round(time.Microsecond))
	}
	fmt.Println("╚═════════════════════════════════════════════════════════════════╝")

	// 性能趋势分析
	fmt.Println("\n📈 性能趋势分析:")
	fmt.Println("==================================")
	stats.mu.Lock()
	if len(stats.history) >= 3 {
		// 取前3秒和最后3秒的平均对比
		var firstAvgTPS, lastAvgTPS float64
		var firstAvgLat, lastAvgLat time.Duration
		for i := 0; i < 3 && i < len(stats.history); i++ {
			firstAvgTPS += stats.history[i].TPS
			firstAvgLat += stats.history[i].AvgLatency
		}
		for i := len(stats.history) - 3; i < len(stats.history); i++ {
			lastAvgTPS += stats.history[i].TPS
			lastAvgLat += stats.history[i].AvgLatency
		}
		firstAvgTPS /= 3
		lastAvgTPS /= 3
		firstAvgLat /= 3
		lastAvgLat /= 3

		tpsChange := (lastAvgTPS - firstAvgTPS) / firstAvgTPS * 100
		latChange := float64(lastAvgLat-firstAvgLat) / float64(firstAvgLat) * 100

		fmt.Printf("├─ 开头 3 秒: 平均 TPS = %.0f, 平均延迟 = %s\n", firstAvgTPS, firstAvgLat.Round(time.Microsecond))
		fmt.Printf("├─ 最后 3 秒: 平均 TPS = %.0f, 平均延迟 = %s\n", lastAvgTPS, lastAvgLat.Round(time.Microsecond))
		fmt.Println("├─────────────────────────────────────────────────────")
		if tpsChange < -20 || latChange > 50 {
			fmt.Printf("└─ ⚠️  检测到性能衰减: TPS 下降 %.0f%%, 延迟上升 %.0f%%\n", -tpsChange, latChange)
		} else if tpsChange > 10 {
			fmt.Printf("└─ ✅ 性能提升: TPS 上升 %.0f%%, 连接池预热完成\n", tpsChange)
		} else {
			fmt.Println("└─ ✅ 性能稳定: 无明显衰减迹象")
		}
	} else {
		fmt.Println("  (测试时间太短，无法分析趋势)")
	}
	history := make([]SecondData, len(stats.history))
	copy(history, stats.history)
	stats.mu.Unlock()

	fmt.Println("\n📋 服务器响应详情:")
	fmt.Println("==================================")

	if len(successSamples) > 0 {
		fmt.Println("✅ 状态码 200 - 响应示例 (来自服务器):")
		fmt.Println("----------------------------------")
		fmt.Println(successSamples[0])
		fmt.Println()
	}

	// 按状态码分组显示错误
	errorByCode := make(map[int][]string)
	for _, e := range errorSamples {
		code := 0
		if len(e) >= 4 && e[0] == '[' {
			if c, err := strconv.Atoi(strings.Trim(e[1:4], "] ")); err == nil {
				code = c
			}
		}
		errorByCode[code] = append(errorByCode[code], e)
	}

	for code, samples := range errorByCode {
		if code == 0 {
			fmt.Println("❌ 网络错误 - 响应示例:")
		} else {
			fmt.Printf("❌ HTTP 状态码 %d - 响应示例 (来自服务器):\n", code)
		}
		fmt.Println("----------------------------------")
		if len(samples) > 0 {
			fmt.Println(samples[0])
		}
		fmt.Println()
	}

	fmt.Println("\n==================================")

	// 是否生成报告
	if plan.Report == "html" || plan.Report == "json" {
		s2xx, s3xx, s4xx, s5xx := stats.GetStatusSummary()
		stats.mu.Lock()
		totalBytes := stats.totalBytes
		stats.mu.Unlock()
		generateReport(plan, total, int(errs), s2xx, s3xx, s4xx, s5xx, avg, p50, p90, p95, p99, min, max, throughput, successSamples, errorSamples, totalBytes, history)
	}
}

// ---------- 报告生成 ----------

type ReportData struct {
	PlanName          string
	TargetURL         string
	Method            string
	Timestamp         string
	Threads           int
	RampUp            int
	Duration          string
	DisableKeepAlives bool
	Total             int
	Success           int
	Errors            int
	SuccessRate       float64
	Status2xx         int64
	Status3xx         int64
	Status4xx         int64
	Status5xx         int64
	Avg               string
	P50               string
	P90               string
	P95               string
	P99               string
	Min               string
	Max               string
	Throughput        float64
	Efficiency        float64
	TotalMB           float64
	SuccessSamples    []string
	ErrorSamples      []string
	// 百分位图表
	ChartLabels []string
	ChartData   []float64
	// 性能趋势图
	HistoryLabels  []string
	HistoryTPS     []float64
	HistoryLatency []float64
}

func generateReport(plan Plan, total, errors int, s2xx, s3xx, s4xx, s5xx int64, avg, p50, p90, p95, p99, min, max time.Duration, throughput float64, successSamples, errorSamples []string, totalBytes int64, history []SecondData) {
	success := total - errors
	successRate := float64(success) * 100 / float64(total)
	efficiency := throughput / float64(plan.Threads)

	// 准备趋势图数据
	historyLabels := make([]string, len(history))
	historyTPS := make([]float64, len(history))
	historyLatency := make([]float64, len(history))
	for i, h := range history {
		historyLabels[i] = fmt.Sprintf("%ds", h.Second)
		historyTPS[i] = math.Round(h.TPS)
		historyLatency[i] = math.Round(float64(h.AvgLatency.Microseconds()) / 1000)
	}

	data := ReportData{
		PlanName:          plan.Name,
		TargetURL:         plan.URL,
		Method:            plan.Method,
		Timestamp:         time.Now().Format("2006-01-02 15:04:05"),
		Threads:           plan.Threads,
		RampUp:            plan.RampUp,
		Duration:          plan.Duration,
		DisableKeepAlives: plan.DisableKeepAlives,
		Total:             total,
		Success:           success,
		Errors:            errors,
		SuccessRate:       math.Round(successRate*100) / 100,
		Status2xx:         s2xx,
		Status3xx:         s3xx,
		Status4xx:         s4xx,
		Status5xx:         s5xx,
		Avg:               avg.Round(time.Microsecond).String(),
		P50:               p50.Round(time.Microsecond).String(),
		P90:               p90.Round(time.Microsecond).String(),
		P95:               p95.Round(time.Microsecond).String(),
		P99:               p99.Round(time.Microsecond).String(),
		Min:               min.Round(time.Microsecond).String(),
		Max:               max.Round(time.Microsecond).String(),
		Throughput:        math.Round(throughput*100) / 100,
		Efficiency:        math.Round(efficiency*10) / 10,
		TotalMB:           math.Round(float64(totalBytes)/1024/1024*100) / 100,
		SuccessSamples:    successSamples,
		ErrorSamples:      errorSamples,
		ChartLabels:       []string{"Min", "P50", "P90", "P95", "P99", "Max"},
		ChartData: []float64{
			float64(min.Microseconds()) / 1000,
			float64(p50.Microseconds()) / 1000,
			float64(p90.Microseconds()) / 1000,
			float64(p95.Microseconds()) / 1000,
			float64(p99.Microseconds()) / 1000,
			float64(max.Microseconds()) / 1000,
		},
		HistoryLabels:  historyLabels,
		HistoryTPS:     historyTPS,
		HistoryLatency: historyLatency,
	}

	var fileName string
	timestamp := time.Now().Format("20060102_150405")
	safeName := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			return r
		}
		return '_'
	}, plan.Name)

	if plan.Report == "html" {
		fileName = fmt.Sprintf("report/%s_%s.html", safeName, timestamp)
		f, err := os.Create(fileName)
		if err != nil {
			fmt.Printf("无法创建报告文件: %v\n", err)
			return
		}
		defer f.Close()
		tmpl := template.Must(template.New("report").Parse(htmlTemplate))
		if err := tmpl.Execute(f, data); err != nil {
			fmt.Printf("生成 HTML 报告失败: %v\n", err)
			return
		}
		fmt.Printf("HTML 报告已生成: %s\n", fileName)
	} else if plan.Report == "json" {
		fileName = fmt.Sprintf("report_%s.json", timestamp)
		f, err := os.Create(fileName)
		if err != nil {
			fmt.Printf("无法创建报告文件: %v\n", err)
			return
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(data); err != nil {
			fmt.Printf("生成 JSON 报告失败: %v\n", err)
			return
		}
		fmt.Printf("JSON 报告已生成: %s\n", fileName)
	}
}

const htmlTemplate = `<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{{.PlanName}} - 性能测试报告</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f7f9fc; }
        .container { max-width: 960px; margin: 0 auto; background: white; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); border-radius: 8px; }
        h1 { color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }
        .summary { display: flex; gap: 20px; margin: 20px 0; flex-wrap: wrap; }
        .card { flex: 1; min-width: 150px; background: #f9fafb; border-left: 5px solid #3498db; padding: 15px; border-radius: 4px; }
        .card h3 { margin: 0 0 8px 0; font-size: 14px; color: #666; }
        .card .value { font-size: 28px; font-weight: bold; color: #2c3e50; }
        .card.fail { border-left-color: #e74c3c; }
        table { width: 100%; border-collapse: collapse; margin: 25px 0; }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
        th { background: #f2f6f9; color: #333; }
        tr:hover { background: #f9f9f9; }
        canvas { margin-top: 30px; }
        .timestamp { color: #888; font-size: 14px; margin-top: -15px; }
    </style>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
</head>
<body>
<div class="container">
    <h1>{{.PlanName}} - 性能测试报告</h1>
    <p class="timestamp">生成时间: {{.Timestamp}}</p>

    <h2>📋 测试配置</h2>
    <table>
        <tr><th width="30%">配置项</th><th>值</th></tr>
        <tr><td>测试名称</td><td><strong>{{.PlanName}}</strong></td></tr>
        <tr><td>目标URL</td><td><code>{{.TargetURL}}</code></td></tr>
        <tr><td>请求方法</td><td><strong>{{.Method}}</strong></td></tr>
        <tr><td>并发线程数</td><td><strong>{{.Threads}}</strong> 线程</td></tr>
        
        {{if eq .RampUp 0}}
        <tr><td>爬坡时间</td><td>立即启动全部线程</td></tr>
        {{else}}
        <tr><td>爬坡时间</td><td><strong>{{.RampUp}}</strong> 秒内逐步启动</td></tr>
        {{end}}
        
        {{if .DisableKeepAlives}}
        <tr><td>连接模式</td><td>🚫 <strong>禁用连接池</strong> （每请求新建连接，模拟浏览器）</td></tr>
        {{else}}
        <tr><td>连接模式</td><td>✅ <strong>启用连接池</strong> （高性能服务器压测）</td></tr>
        {{end}}
        <tr><td>压测时长</td><td><strong>{{.Duration}}</strong></td></tr>
    </table>

    <h2>📊 核心指标</h2>
    <div class="summary">
        <div class="card">
            <h3>总请求数</h3>
            <div class="value">{{.Total}}</div>
        </div>
        <div class="card" style="border-left-color: #2ecc71;">
            <h3>成功率</h3>
            <div class="value">{{.SuccessRate}}%</div>
        </div>
        <div class="card" style="border-left-color: #e74c3c;">
            <h3>失败数</h3>
            <div class="value">{{.Errors}}</div>
        </div>
        <div class="card">
            <h3>吞吐量</h3>
            <div class="value">{{.Throughput}}/s</div>
        </div>
        <div class="card">
            <h3>单线程效率</h3>
            <div class="value">{{.Efficiency}}/s</div>
        </div>
        <div class="card">
            <h3>总数据量</h3>
            <div class="value">{{.TotalMB}} MB</div>
        </div>
    </div>

    <h2>📈 状态码统计</h2>
    <div class="summary">
        <div class="card" style="border-left-color: #2ecc71;">
            <h3>2xx 成功</h3>
            <div class="value">{{.Status2xx}}</div>
        </div>
        <div class="card" style="border-left-color: #f39c12;">
            <h3>3xx 重定向</h3>
            <div class="value">{{.Status3xx}}</div>
        </div>
        <div class="card" style="border-left-color: #e67e22;">
            <h3>4xx 客户端错误</h3>
            <div class="value">{{.Status4xx}}</div>
        </div>
        <div class="card fail">
            <h3>5xx 服务器错误</h3>
            <div class="value">{{.Status5xx}}</div>
        </div>
    </div>

    <h2>⏱️ 响应时间</h2>
    <div class="summary">
        <div class="card">
            <h3>平均响应</h3>
            <div class="value">{{.Avg}}</div>
        </div>
    </div>

    <h2>HTTP 状态码说明对照表</h2>
    <table>
        <tr><th>状态码</th><th>官方说明</th><th>含义</th></tr>
        <tr style="background: #d5f5e3;"><td><strong>200</strong></td><td>OK</td><td>✅ 请求成功</td></tr>
        <tr style="background: #d5f5e3;"><td><strong>201</strong></td><td>Created</td><td>✅ 创建成功</td></tr>
        <tr style="background: #fef9e7;"><td><strong>301</strong></td><td>Moved Permanently</td><td>⚠️ 永久重定向</td></tr>
        <tr style="background: #fef9e7;"><td><strong>302</strong></td><td>Found</td><td>⚠️ 临时重定向</td></tr>
        <tr style="background: #fdedec;"><td><strong>400</strong></td><td>Bad Request</td><td>❌ 请求参数错误</td></tr>
        <tr style="background: #fdedec;"><td><strong>401</strong></td><td>Unauthorized</td><td>❌ 未登录/Token无效</td></tr>
        <tr style="background: #fdedec;"><td><strong>403</strong></td><td>Forbidden</td><td>❌ 没有权限</td></tr>
        <tr style="background: #fdedec;"><td><strong>404</strong></td><td>Not Found</td><td>❌ 接口不存在</td></tr>
        <tr style="background: #fdedec;"><td><strong>429</strong></td><td>Too Many Requests</td><td>❌ 被限流了</td></tr>
        <tr style="background: #f5b7b1;"><td><strong>500</strong></td><td>Internal Server Error</td><td>💥 服务器崩了</td></tr>
        <tr style="background: #f5b7b1;"><td><strong>502</strong></td><td>Bad Gateway</td><td>💥 网关错误</td></tr>
        <tr style="background: #f5b7b1;"><td><strong>503</strong></td><td>Service Unavailable</td><td>💥 服务挂了</td></tr>
        <tr style="background: #f5b7b1;"><td><strong>504</strong></td><td>Gateway Timeout</td><td>💥 网关超时</td></tr>
    </table>

    <h2>响应时间百分位</h2>
    <table>
        <tr><th>指标</th><th>耗时</th></tr>
        <tr><td>最小</td><td>{{.Min}}</td></tr>
        <tr><td>P50</td><td>{{.P50}}</td></tr>
        <tr><td>P90</td><td>{{.P90}}</td></tr>
        <tr><td>P95</td><td>{{.P95}}</td></tr>
        <tr><td>P99</td><td>{{.P99}}</td></tr>
        <tr><td>最大</td><td>{{.Max}}</td></tr>
    </table>

    <h2>📈 性能趋势图 (双轴)</h2>
    <canvas id="trendChart" height="120"></canvas>

    <h2>百分位图 (毫秒)</h2>
    <canvas id="percentileChart" width="400" height="200"></canvas>

    {{if .SuccessSamples}}
    <h2 style="margin-top: 40px;">✅ 成功响应示例</h2>
    {{range .SuccessSamples}}
    <pre style="background: #f8f9fa; padding: 15px; border-radius: 4px; border-left: 4px solid #2ecc71; overflow-x: auto; font-size: 12px;">{{.}}</pre>
    {{end}}
    {{end}}

    {{if .ErrorSamples}}
    <h2 style="margin-top: 40px;">❌ 错误响应示例</h2>
    {{range .ErrorSamples}}
    <pre style="background: #fdf3f3; padding: 15px; border-radius: 4px; border-left: 4px solid #e74c3c; overflow-x: auto; font-size: 12px;">{{.}}</pre>
    {{end}}
{{end}}
</div>
<script>
    // 性能趋势双轴图
    const trendCtx = document.getElementById('trendChart').getContext('2d');
    new Chart(trendCtx, {
        type: 'line',
        data: {
            labels: {{.HistoryLabels}},
            datasets: [
                {
                    label: 'TPS 吞吐量',
                    data: {{.HistoryTPS}},
                    borderColor: '#3498db',
                    backgroundColor: 'rgba(52, 152, 219, 0.1)',
                    fill: true,
                    tension: 0.3,
                    yAxisID: 'y'
                },
                {
                    label: '响应时间 (ms)',
                    data: {{.HistoryLatency}},
                    borderColor: '#e74c3c',
                    backgroundColor: 'rgba(231, 76, 60, 0.1)',
                    fill: true,
                    tension: 0.3,
                    yAxisID: 'y1'
                }
            ]
        },
        options: {
            interaction: { mode: 'index', intersect: false },
            scales: {
                y: {
                    type: 'linear',
                    position: 'left',
                    title: { display: true, text: 'TPS 请求/秒' }
                },
                y1: {
                    type: 'linear',
                    position: 'right',
                    title: { display: true, text: '响应时间 毫秒' },
                    grid: { drawOnChartArea: false }
                }
            }
        }
    });

    // 百分位柱状图
    const ctx = document.getElementById('percentileChart').getContext('2d');
    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: {{.ChartLabels}},
            datasets: [{
                label: '响应时间 (ms)',
                data: {{.ChartData}},
                backgroundColor: 'rgba(52, 152, 219, 0.6)',
                borderColor: 'rgba(52, 152, 219, 1)',
                borderWidth: 1
            }]
        },
        options: {
            scales: {
                y: { beginAtZero: true, title: { display: true, text: '毫秒' } }
            }
        }
    });
</script>
</body>
</html>`

// ---------- 交互式创建 ----------

func readInput(reader *bufio.Reader, prompt string) string {
	fmt.Print(prompt)
	input, _ := reader.ReadString('\n')
	return strings.TrimSpace(input)
}

func interactiveBuild() Plan {
	reader := bufio.NewReader(os.Stdin)
	plan := Plan{Name: "Untitled", Method: "GET", Threads: 10, Duration: "30s", Report: "html"}

	fmt.Println("=== 交互式创建性能测试计划 ===")
	fmt.Println("💡 提示: 任意步骤输入 'b' 可以回退到上一步")

	steps := []struct {
		name string
		exec func() bool
	}{
		{
			name: "测试名称",
			exec: func() bool {
				input := readInput(reader, fmt.Sprintf("测试名称 (默认 %s): ", plan.Name))
				if input == "b" {
					return false
				}
				if input != "" {
					plan.Name = input
				}
				return true
			},
		},
		{
			name: "目标URL",
			exec: func() bool {
				for {
					input := readInput(reader, "目标 URL: ")
					if input == "b" {
						return false
					}
					if input != "" {
						plan.URL = input
						break
					}
					fmt.Println("URL 不能为空!")
				}
				return true
			},
		},
		{
			name: "请求方法",
			exec: func() bool {
				input := readInput(reader, fmt.Sprintf("请求方法 (默认 %s): ", plan.Method))
				if input == "b" {
					return false
				}
				if input != "" {
					plan.Method = strings.ToUpper(input)
				}
				return true
			},
		},
		{
			name: "Header",
			exec: func() bool {
				plan.Headers = make(map[string]string)
				for {
					input := readInput(reader, "Header 键 (回车跳过, b=回退): ")
					if input == "b" {
						return false
					}
					if input == "" {
						break
					}
					val := readInput(reader, "Header 值: ")
					plan.Headers[input] = val
				}
				return true
			},
		},
		{
			name: "请求Body",
			exec: func() bool {
				input := readInput(reader, "请求 Body (用于 POST/PUT, 可直接回车, b=回退): ")
				if input == "b" {
					return false
				}
				plan.Body = input
				return true
			},
		},
		{
			name: "并发线程数",
			exec: func() bool {
				input := readInput(reader, fmt.Sprintf("并发线程数 (默认 %d, b=回退): ", plan.Threads))
				if input == "b" {
					return false
				}
				if input != "" {
					if t, err := strconv.Atoi(input); err == nil {
						plan.Threads = t
					}
				}
				return true
			},
		},
		{
			name: "爬坡时间",
			exec: func() bool {
				input := readInput(reader, "爬坡时间(秒) - 线程数逐步上升 (0=立刻启动, 默认 0, b=回退): ")
				if input == "b" {
					return false
				}
				if input != "" {
					if r, err := strconv.Atoi(input); err == nil {
						plan.RampUp = r
					}
				}
				return true
			},
		},
		{
			name: "持续时间",
			exec: func() bool {
				input := readInput(reader, fmt.Sprintf("持续时间 (如 30s, 1m, 默认 %s, b=回退): ", plan.Duration))
				if input == "b" {
					return false
				}
				if input != "" {
					plan.Duration = input
				}
				return true
			},
		},
		{
			name: "连接模式",
			exec: func() bool {
				input := readInput(reader, "连接模式: n=禁用连接池(模拟浏览器), y=复用连接(默认, b=回退): ")
				if input == "b" {
					return false
				}
				if input == "n" {
					plan.DisableKeepAlives = true
				}
				return true
			},
		},
		{
			name: "报告格式",
			exec: func() bool {
				input := readInput(reader, fmt.Sprintf("报告格式 (html/json, 默认 %s, b=回退): ", plan.Report))
				if input == "b" {
					return false
				}
				if input != "" {
					plan.Report = input
				}
				return true
			},
		},
	}

	currentStep := 0
	for currentStep < len(steps) {
		success := steps[currentStep].exec()
		if success {
			currentStep++
		} else {
			if currentStep > 0 {
				currentStep--
				fmt.Printf("⬅️  回退到: %s\n", steps[currentStep].name)
			}
		}
	}

	// 保存为 JSON 文件
	saveJSON(plan)

	return plan
}

func saveJSON(plan Plan) {
	fmt.Print("保存为 JSON 文件? (y/n, 默认 y): ")
	reader := bufio.NewReader(os.Stdin)
	ans, _ := reader.ReadString('\n')
	ans = strings.TrimSpace(ans)
	if ans == "n" || ans == "N" {
		return
	}
	fileName := plan.Name + ".json"
	f, err := os.Create(fileName)
	if err != nil {
		fmt.Printf("无法保存文件: %v\n", err)
		return
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(plan); err != nil {
		fmt.Printf("保存失败: %v\n", err)
		return
	}
	fmt.Printf("计划已保存为 %s\n", fileName)
}

func pressAnyKey() {
	fmt.Println("\n按回车键继续...")
	bufio.NewReader(os.Stdin).ReadBytes('\n')
}

func selectConfigFile() (Plan, bool) {
	fmt.Println("\n========== 发现以下测试配置 ==========")
	files, _ := filepath.Glob("*.json")
	if len(files) == 0 {
		fmt.Println("  (没有找到已保存的配置文件)")
		fmt.Println("======================================")
		return Plan{}, false
	}
	for i, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var plan Plan
		if err := json.Unmarshal(data, &plan); err != nil {
			continue
		}
		fmt.Printf("  %d. %-20s (%d线程, %s) %s\n", i+1, plan.Name, plan.Threads, plan.Duration, f)
	}
	fmt.Printf("  0. 不导入，创建新测试\n")
	fmt.Println("======================================")
	fmt.Print("请选择配置 (输入编号): ")

	reader := bufio.NewReader(os.Stdin)
	choiceStr, _ := reader.ReadString('\n')
	choiceStr = strings.TrimSpace(choiceStr)
	choice, err := strconv.Atoi(choiceStr)
	if err != nil || choice < 0 || choice > len(files) {
		fmt.Println("无效选择，创建新测试...")
		return Plan{}, false
	}
	if choice == 0 {
		return Plan{}, false
	}

	data, _ := os.ReadFile(files[choice-1])
	var plan Plan
	json.Unmarshal(data, &plan)
	fmt.Printf("✓ 已加载配置: %s\n", plan.Name)
	return plan, true
}

func showMainMenu() {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("\n")
		fmt.Println("==================================")
		fmt.Println("        性能压测工具主菜单")
		fmt.Println("==================================")
		fmt.Println("1. 新建测试计划 (完整向导)")
		fmt.Println("2. 选择配置文件运行")
		fmt.Println("3. 快速测试 (输入URL直接压测)")
		fmt.Println("0. 退出程序")
		fmt.Println("==================================")
		fmt.Print("请选择选项 (0-3): ")

		choice, _ := reader.ReadString('\n')
		choice = strings.TrimSpace(choice)

		switch choice {
		case "1":
			fmt.Println()
			plan, hasConfig := selectConfigFile()
			if !hasConfig {
				plan = interactiveBuild()
			}
			runPlan(plan)
			pressAnyKey()
		case "2":
			fmt.Println("\n========== 可用的配置文件 ==========")
			files, _ := filepath.Glob("*.json")
			if len(files) == 0 {
				fmt.Println("当前目录没有找到 JSON 配置文件")
				fmt.Println("请先通过选项 1 交互式创建并保存配置")
				pressAnyKey()
				continue
			}
			for i, f := range files {
				fmt.Printf("  %d. %s\n", i+1, f)
			}
			fmt.Printf("  0. 返回主菜单\n")
			fmt.Println("==================================")
			fmt.Print("请选择编号: ")

			choiceStr, _ := reader.ReadString('\n')
			choiceStr = strings.TrimSpace(choiceStr)
			choice, err := strconv.Atoi(choiceStr)
			if err != nil || choice < 0 || choice > len(files) {
				fmt.Println("无效的编号")
				pressAnyKey()
				continue
			}
			if choice == 0 {
				continue
			}
			fileName := files[choice-1]
			fmt.Printf("已选择: %s\n", fileName)

			data, err := os.ReadFile(fileName)
			if err != nil {
				fmt.Printf("无法读取文件: %v\n", err)
				pressAnyKey()
				continue
			}
			var plan Plan
			if err := json.Unmarshal(data, &plan); err != nil {
				fmt.Printf("JSON 解析失败: %v\n", err)
				pressAnyKey()
				continue
			}
			runPlan(plan)
			pressAnyKey()
		case "3":
			fmt.Print("\n输入目标 URL: ")
			url, _ := reader.ReadString('\n')
			url = strings.TrimSpace(url)
			if url == "" {
				fmt.Println("URL 不能为空")
				pressAnyKey()
				continue
			}
			plan := Plan{
				Name:     "Quick Test",
				URL:      url,
				Method:   "GET",
				Threads:  10,
				Duration: "30s",
				Report:   "html",
			}
			runPlan(plan)
			pressAnyKey()
		case "0":
			fmt.Println("感谢使用，再见！")
			return
		default:
			fmt.Println("无效选项，请重新选择")
		}
	}
}

// ---------- 命令行解析 ----------

func main() {
	// 子命令：quick, run
	quickCmd := flag.NewFlagSet("quick", flag.ExitOnError)
	quickConcurrency := quickCmd.Int("c", 10, "并发数")
	quickDuration := quickCmd.String("d", "30s", "持续时间")
	quickMethod := quickCmd.String("X", "GET", "HTTP 方法")
	quickHeaders := quickCmd.String("H", "", "HTTP 头，格式 key:value，多个用逗号分隔")
	quickBody := quickCmd.String("b", "", "请求体")

	runCmd := flag.NewFlagSet("run", flag.ExitOnError)
	runReport := runCmd.String("report", "html", "报告类型 (html/json)")

	if len(os.Args) < 2 {
		// 无参数，进入主菜单
		showMainMenu()
		return
	}

	switch os.Args[1] {
	case "quick":
		quickCmd.Parse(os.Args[2:])
		// quick 命令需要一个 URL 作为最后一个参数
		args := quickCmd.Args()
		if len(args) < 1 {
			fmt.Println("用法: perftest quick [options] <URL>")
			pressAnyKey()
			return
		}
		url := args[0]
		// 解析 headers
		headers := make(map[string]string)
		if *quickHeaders != "" {
			pairs := strings.Split(*quickHeaders, ",")
			for _, p := range pairs {
				kv := strings.SplitN(p, ":", 2)
				if len(kv) == 2 {
					headers[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
				}
			}
		}
		plan := Plan{
			Name:     "Quick Test",
			URL:      url,
			Method:   strings.ToUpper(*quickMethod),
			Headers:  headers,
			Body:     *quickBody,
			Threads:  *quickConcurrency,
			Duration: *quickDuration,
			Report:   "html", // 默认生成 html
		}
		runPlan(plan)
		pressAnyKey()

	case "run":
		runCmd.Parse(os.Args[2:])
		args := runCmd.Args()
		if len(args) < 1 {
			fmt.Println("用法: perftest run [--report html|json] <plan.json>")
			pressAnyKey()
			return
		}
		fileName := args[0]
		data, err := os.ReadFile(fileName)
		if err != nil {
			fmt.Printf("无法读取文件: %v\n", err)
			pressAnyKey()
			return
		}
		var plan Plan
		if err := json.Unmarshal(data, &plan); err != nil {
			fmt.Printf("JSON 解析失败: %v\n", err)
			pressAnyKey()
			return
		}
		if *runReport != "" {
			plan.Report = *runReport
		}
		runPlan(plan)
		pressAnyKey()

	default:
		fmt.Println("未知命令。可用: quick, run 或直接运行进入交互模式")
		pressAnyKey()
	}
}
