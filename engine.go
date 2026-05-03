package main

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func RunPlan(plan Plan) {
	dur, err := time.ParseDuration(plan.Duration)
	if err != nil {
		fmt.Printf("无效的 duration: %s\n", plan.Duration)
		return
	}

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
			return http.ErrUseLastResponse
		},
	}

	stats := NewStatsCollector()
	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	go printStatsLoop(stats, stopChan)

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

	startTime := time.Now()
	rampInterval := time.Duration(0)
	if plan.RampUp > 0 && plan.Threads > 0 {
		rampInterval = time.Duration(plan.RampUp*1000/plan.Threads) * time.Millisecond
		fmt.Printf("🎚️  爬坡模式: 每 %v 启动 1 线程\n", rampInterval)
	}

	for i := 0; i < plan.Threads; i++ {
		wg.Add(1)
		go worker(i, &wg, plan, client, stats, startTime, dur, rampInterval, userStop)
	}

	wg.Wait()
	close(stopChan)

	printFinalSummary(plan, stats, dur)

	if plan.Report == "html" || plan.Report == "json" {
		s2xx, s3xx, s4xx, s5xx := stats.GetStatusSummary()
		total, errs, avg, p50, p90, p95, p99, min, max := stats.Snapshot()
		successSamples, errorSamples := stats.GetSamples()
		throughput := float64(total) / dur.Seconds()
		totalBytes := stats.GetTotalBytes()
		history := stats.GetHistory()
		GenerateReport(plan, total, int(errs), s2xx, s3xx, s4xx, s5xx, avg, p50, p90, p95, p99, min, max, throughput, successSamples, errorSamples, totalBytes, history)
	}
}

func printStatsLoop(stats *StatsCollector, stopChan <-chan struct{}) {
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

			deltaReq := total - lastTotal
			deltaTime := time.Since(lastTime).Seconds()
			tps := float64(deltaReq) / deltaTime
			errRate := float64(errs) * 100 / float64(total)
			if total == 0 {
				errRate = 0
			}
			second := int(time.Since(start).Seconds()) + 1

			stats.AddHistory(SecondData{
				Second:     second,
				TPS:        tps,
				AvgLatency: avg,
				Errors:     int(errs),
				TotalReqs:  int(total),
			})

			fmt.Printf("[%ds] 总量:%-5d | 每秒:%-4.0f | 平均:%-8s | 成功:%-4d 2xx:%-4d 3xx:%-4d 4xx:%-4d 5xx:%-4d | 错误率:%.1f%%\n",
				second, total, tps, avg.Round(time.Microsecond),
				total-int(errs), s2xx, s3xx, s4xx, s5xx, errRate)

			lastTotal = total
			lastTime = time.Now()
		case <-stopChan:
			return
		}
	}
}

func worker(id int, wg *sync.WaitGroup, plan Plan, client *http.Client, stats *StatsCollector, startTime time.Time, dur time.Duration, rampInterval time.Duration, userStop <-chan struct{}) {
	defer wg.Done()
	if rampInterval > 0 {
		select {
		case <-time.After(time.Duration(id) * rampInterval):
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
}

func printFinalSummary(plan Plan, stats *StatsCollector, dur time.Duration) {
	total, errs, avg, p50, p90, p95, p99, min, max := stats.Snapshot()
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
	fmt.Printf("║  📦 总数据:    %-18.2f MB                           ║\n", float64(stats.GetTotalBytes())/1024/1024)
	fmt.Println("║                HTTP 状态码明细                                 ║")
	fmt.Println("╠════════════╤══════════╤══════════════════════════════════════╣")
	fmt.Println("║   状态码   │   数量   │              说明                  ║")
	fmt.Println("╟────────────┼──────────┼──────────────────────────────────────╢")

	stats.mu.Lock()
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
	fmt.Println("╚════════════╧══════════╧══════════════════════════════════════╝")

	fmt.Println()
	fmt.Println("╔═════════════════════════════════════════════════════════════════╗")
	fmt.Printf("║  ⏱️   平均:  %-12s  │  P50:  %-12s  │  P90:  %-16s  ║\n",
		avg.Round(time.Microsecond), p50.Round(time.Microsecond), p90.Round(time.Microsecond))
	fmt.Printf("║  ⏱️   P95:   %-12s  │  P99:  %-12s  │  最小:  %-12s    ║\n",
		p95.Round(time.Microsecond), p99.Round(time.Microsecond), min.Round(time.Microsecond))
	fmt.Printf("║  ⏱️   最大:  %-54s    ║\n", max.Round(time.Microsecond))
	fmt.Println("╚═════════════════════════════════════════════════════════════════╝")

	printTrendAnalysis(stats.GetHistory())
	printResponseSamples(stats)
}

func printTrendAnalysis(history []SecondData) {
	fmt.Println("\n📈 性能趋势分析:")
	fmt.Println("==================================")
	if len(history) >= 3 {
		var firstAvgTPS, lastAvgTPS float64
		var firstAvgLat, lastAvgLat time.Duration
		for i := 0; i < 3 && i < len(history); i++ {
			firstAvgTPS += history[i].TPS
			firstAvgLat += history[i].AvgLatency
		}
		for i := len(history) - 3; i < len(history); i++ {
			lastAvgTPS += history[i].TPS
			lastAvgLat += history[i].AvgLatency
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
}

func printResponseSamples(stats *StatsCollector) {
	successSamples, errorSamples := stats.GetSamples()

	fmt.Println("\n📋 服务器响应详情:")
	fmt.Println("==================================")

	if len(successSamples) > 0 {
		fmt.Println("✅ 状态码 200 - 响应示例 (来自服务器):")
		fmt.Println("----------------------------------")
		fmt.Println(successSamples[0])
		fmt.Println()
	}

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
}
