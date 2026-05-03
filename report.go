package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"math"
	"os"
	"strings"
	"time"
)

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
	ChartLabels       []string
	ChartData         []float64
	HistoryLabels     []string
	HistoryTPS        []float64
	HistoryLatency    []float64
}

func GenerateReport(plan Plan, total, errors int, s2xx, s3xx, s4xx, s5xx int64, avg, p50, p90, p95, p99, min, max time.Duration, throughput float64, successSamples, errorSamples []string, totalBytes int64, history []SecondData) {
	success := total - errors
	successRate := float64(success) * 100 / float64(total)
	efficiency := throughput / float64(plan.Threads)

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

	os.MkdirAll("report", 0755)

	timestamp := time.Now().Format("20060102_150405")
	safeName := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			return r
		}
		return '_'
	}, plan.Name)

	if plan.Report == "html" {
		fileName := fmt.Sprintf("report/%s_%s.html", safeName, timestamp)
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
		fileName := fmt.Sprintf("report/%s_%s.json", safeName, timestamp)
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
    </table>

    <h2>📈 性能趋势图</h2>
    <canvas id="trendChart" height="300"></canvas>
    <script>
        var ctx = document.getElementById('trendChart').getContext('2d');
        new Chart(ctx, {
            type: 'line',
            data: {
                labels: {{.HistoryLabels}},
                datasets: [{
                    label: '吞吐量 (req/s)',
                    data: {{.HistoryTPS}},
                    borderColor: '#3498db',
                    backgroundColor: 'rgba(52, 152, 219, 0.1)',
                    tension: 0.3,
                    yAxisID: 'y'
                }, {
                    label: '响应时间 (ms)',
                    data: {{.HistoryLatency}},
                    borderColor: '#e74c3c',
                    backgroundColor: 'rgba(231, 76, 60, 0.1)',
                    tension: 0.3,
                    yAxisID: 'y1'
                }]
            },
            options: {
                scales: {
                    y: {
                        type: 'linear',
                        position: 'left',
                        title: { display: true, text: '吞吐量 (req/s)' }
                    },
                    y1: {
                        type: 'linear',
                        position: 'right',
                        title: { display: true, text: '响应时间 (ms)' },
                        grid: { drawOnChartArea: false }
                    }
                }
            }
        });
    </script>
</div>
</body>
</html>`
