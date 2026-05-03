package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Plan struct {
	Name              string            `json:"name"`
	URL               string            `json:"url"`
	Method            string            `json:"method"`
	Headers           map[string]string `json:"headers,omitempty"`
	Body              string            `json:"body,omitempty"`
	Threads           int               `json:"threads"`
	RampUp            int               `json:"rampup"`
	Duration          string            `json:"duration"`
	Report            string            `json:"report,omitempty"`
	DisableKeepAlives bool              `json:"disable_keepalives"`
}

func main() {
	os.MkdirAll("report", 0755)

	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "quick":
			quickCommand()
		case "import":
			importCommand()
		default:
			fmt.Println("未知命令:", os.Args[1])
			os.Exit(1)
		}
		return
	}

	mainMenu()
}

func mainMenu() {
	for {
		fmt.Println()
		fmt.Println("╔═══════════════════════════════════════╗")
		fmt.Println("║       HTTP 性能测试工具 v3.1         ║")
		fmt.Println("╠═══════════════════════════════════════╣")
		fmt.Println("║  1. 交互式创建测试计划               ║")
		fmt.Println("║  2. 从文件导入测试计划               ║")
		fmt.Println("║  3. 列出可用的 JSON 配置文件         ║")
		fmt.Println("║  0. 退出                             ║")
		fmt.Println("╚═══════════════════════════════════════╝")
		fmt.Print("请选择: ")

		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		choice := strings.TrimSpace(scanner.Text())

		switch choice {
		case "1":
			runInteractive()
		case "2":
			importCommand()
		case "3":
			listJSONFiles()
		case "0", "q", "Q":
			fmt.Println("再见!")
			return
		default:
			fmt.Println("无效输入")
		}
		pressAnyKey()
	}
}

func runInteractive() {
	scanner := bufio.NewScanner(os.Stdin)
	step := 0
	plan := Plan{}

	backStack := make([]Plan, 0)

	for step < 8 {
		backStack = append(backStack, plan)

		switch step {
		case 0:
			fmt.Print("\n📋 测试名称: ")
			scanner.Scan()
			input := strings.TrimSpace(scanner.Text())
			if input == "b" {
				return
			}
			if input == "s" {
				step++
				continue
			}
			plan.Name = input
			if plan.Name == "" {
				plan.Name = "Quick Test"
			}
		case 1:
			fmt.Print("🌐 目标 URL: ")
			scanner.Scan()
			input := strings.TrimSpace(scanner.Text())
			if input == "b" {
				step, plan = rollback(step, backStack)
				backStack = backStack[:len(backStack)-1]
				continue
			}
			plan.URL = input
		case 2:
			fmt.Print("📝 请求方法 [GET/POST] (默认 GET): ")
			scanner.Scan()
			input := strings.TrimSpace(scanner.Text())
			if input == "b" {
				step, plan = rollback(step, backStack)
				backStack = backStack[:len(backStack)-1]
				continue
			}
			if input == "" {
				plan.Method = "GET"
			} else {
				plan.Method = strings.ToUpper(input)
			}
		case 3:
			fmt.Print("🔢 并发线程数 (默认 10): ")
			scanner.Scan()
			input := strings.TrimSpace(scanner.Text())
			if input == "b" {
				step, plan = rollback(step, backStack)
				backStack = backStack[:len(backStack)-1]
				continue
			}
			if input == "" {
				plan.Threads = 10
			} else {
				plan.Threads, _ = strconv.Atoi(input)
				if plan.Threads <= 0 {
					plan.Threads = 10
				}
			}
		case 4:
			fmt.Print("📈 爬坡时间(秒), 0=立即启动 (默认 0): ")
			scanner.Scan()
			input := strings.TrimSpace(scanner.Text())
			if input == "b" {
				step, plan = rollback(step, backStack)
				backStack = backStack[:len(backStack)-1]
				continue
			}
			if input == "" {
				plan.RampUp = 0
			} else {
				plan.RampUp, _ = strconv.Atoi(input)
			}
		case 5:
			fmt.Print("⏱️  压测时长 如 30s, 1m, 5m (默认 30s): ")
			scanner.Scan()
			input := strings.TrimSpace(scanner.Text())
			if input == "b" {
				step, plan = rollback(step, backStack)
				backStack = backStack[:len(backStack)-1]
				continue
			}
			if input == "" {
				plan.Duration = "30s"
			} else {
				plan.Duration = input
			}
		case 6:
			fmt.Print("🔌 连接复用 y=连接池(高性能)/n=禁用(测NAT) (默认 y): ")
			scanner.Scan()
			input := strings.TrimSpace(scanner.Text())
			if input == "b" {
				step, plan = rollback(step, backStack)
				backStack = backStack[:len(backStack)-1]
				continue
			}
			if input == "n" || input == "N" {
				plan.DisableKeepAlives = true
			} else {
				plan.DisableKeepAlives = false
			}
		case 7:
			fmt.Print("📄 生成报告 html/none (默认 html): ")
			scanner.Scan()
			input := strings.TrimSpace(scanner.Text())
			if input == "b" {
				step, plan = rollback(step, backStack)
				backStack = backStack[:len(backStack)-1]
				continue
			}
			if input == "none" {
				plan.Report = ""
			} else {
				plan.Report = "html"
			}
		}
		step++
	}

	fmt.Println()
	fmt.Println("✅ 测试配置确认")
	fmt.Println("==================================")
	fmt.Printf("   名称: %s\n", plan.Name)
	fmt.Printf("   URL: %s\n", plan.URL)
	fmt.Printf("   方法: %s\n", plan.Method)
	fmt.Printf("   线程数: %d\n", plan.Threads)
	fmt.Printf("   爬坡: %ds\n", plan.RampUp)
	fmt.Printf("   时长: %s\n", plan.Duration)
	if plan.DisableKeepAlives {
		fmt.Println("   连接池: 禁用 (模拟浏览器)")
	} else {
		fmt.Println("   连接池: 启用 (高性能)")
	}
	fmt.Printf("   报告: %s\n", plan.Report)
	fmt.Println()

	fmt.Print("保存配置到 JSON? (y/n): ")
	scanner.Scan()
	if strings.ToLower(strings.TrimSpace(scanner.Text())) == "y" {
		data, _ := json.MarshalIndent(plan, "", "  ")
		os.WriteFile(plan.Name+".json", data, 0644)
		fmt.Println("已保存到", plan.Name+".json")
	}
	fmt.Println()
	fmt.Println("🚀 开始压测...")
	fmt.Println()

	RunPlan(plan)
}

func rollback(step int, stack []Plan) (int, Plan) {
	if step <= 1 {
		return 0, stack[0]
	}
	return step - 2, stack[step-2]
}

func quickCommand() {
	fs := flag.NewFlagSet("quick", flag.ExitOnError)
	concurrency := fs.Int("c", 10, "并发数")
	duration := fs.String("d", "30s", "时长如 30s, 1m")
	keepAlive := fs.String("k", "y", "y=复用连接 n=禁用")
	method := fs.String("m", "GET", "请求方法")
	fs.Parse(os.Args[2:])

	if fs.NArg() == 0 {
		fmt.Println("请指定 URL")
		os.Exit(1)
	}

	plan := Plan{
		Name:              "Quick Test",
		URL:               fs.Arg(0),
		Method:            strings.ToUpper(*method),
		Threads:           *concurrency,
		RampUp:            0,
		Duration:          *duration,
		DisableKeepAlives: *keepAlive == "n",
		Report:            "html",
	}
	RunPlan(plan)
}

func importCommand() {
	matches, _ := filepath.Glob("*.json")
	if len(matches) == 0 {
		fmt.Println("当前目录没有 JSON 配置文件")
		return
	}

	fmt.Println("找到以下配置文件:")
	for i, m := range matches {
		fmt.Printf("  %d. %s\n", i+1, m)
	}
	fmt.Print("请输入编号: ")

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	idx, _ := strconv.Atoi(strings.TrimSpace(scanner.Text()))
	if idx < 1 || idx > len(matches) {
		fmt.Println("无效编号")
		return
	}

	data, err := os.ReadFile(matches[idx-1])
	if err != nil {
		fmt.Println("读取文件失败:", err)
		return
	}

	var plan Plan
	if err := json.Unmarshal(data, &plan); err != nil {
		fmt.Println("解析 JSON 失败:", err)
		return
	}

	fmt.Printf("已加载: %s\n", plan.Name)
	pressAnyKey()
	RunPlan(plan)
}

func listJSONFiles() {
	matches, _ := filepath.Glob("*.json")
	if len(matches) == 0 {
		fmt.Println("当前目录没有 JSON 配置文件")
		return
	}
	fmt.Println("找到以下配置文件:")
	for _, m := range matches {
		data, err := os.ReadFile(m)
		if err != nil {
			continue
		}
		var plan Plan
		if json.Unmarshal(data, &plan) == nil {
			fmt.Printf("  %-25s  %d 线程 × %s\n", m, plan.Threads, plan.Duration)
		}
	}
}

func pressAnyKey() {
	fmt.Print("\n按回车键继续...")
	bufio.NewReader(os.Stdin).ReadString('\n')
}
