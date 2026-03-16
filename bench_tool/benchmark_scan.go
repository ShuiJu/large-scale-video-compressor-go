package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

// ================== 配置 ==================

// 与主程序保持一致的视频扩展名集合
var videoExtensions = map[string]bool{
	".mp4": true,
	".mov": true,
	".avi": true,
	".mkv": true,
	".wmv": true,
	".m4v": true,
}

// 与主程序保持一致的跳过规则
func shouldSkip(base string) bool {
	stem := strings.TrimSuffix(base, filepath.Ext(base))
	return strings.Contains(base, ".__tmp__") ||
		strings.HasSuffix(stem, "_av1_6000k") ||
		strings.HasSuffix(stem, "_svt-av1_3000k") ||
		strings.HasSuffix(stem, "_copy_original")
}

// ================== 方案一：串行 Walk ==================

func scanSerial(rootDir string) ([]string, error) {
	var files []string
	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			return nil
		}
		if !videoExtensions[strings.ToLower(filepath.Ext(path))] {
			return nil
		}
		if shouldSkip(filepath.Base(path)) {
			return nil
		}
		files = append(files, path)
		return nil
	})
	return files, err
}

// ================== 方案二：并发 Walk（顶层子目录各一个 goroutine）==================

func scanParallel(rootDir string) ([]string, error) {
	// 列出顶层直接子目录
	entries, err := os.ReadDir(rootDir)
	if err != nil {
		return nil, fmt.Errorf("读取根目录失败: %w", err)
	}

	type walkResult struct {
		files []string
		err   error
	}

	var topDirs []string
	var rootFiles []string // 根目录下直接存在的视频文件（不在子目录中）

	for _, e := range entries {
		if e.IsDir() {
			topDirs = append(topDirs, filepath.Join(rootDir, e.Name()))
		} else {
			// 根目录直接放的文件也要处理
			name := e.Name()
			if videoExtensions[strings.ToLower(filepath.Ext(name))] && !shouldSkip(name) {
				rootFiles = append(rootFiles, filepath.Join(rootDir, name))
			}
		}
	}

	if len(topDirs) == 0 {
		// 没有子目录，退化为串行
		return scanSerial(rootDir)
	}

	resultCh := make(chan walkResult, len(topDirs))
	var wg sync.WaitGroup

	for _, dir := range topDirs {
		wg.Add(1)
		go func(d string) {
			defer wg.Done()
			var files []string
			walkErr := filepath.Walk(d, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return nil
				}
				if info.IsDir() {
					return nil
				}
				if !videoExtensions[strings.ToLower(filepath.Ext(path))] {
					return nil
				}
				if shouldSkip(filepath.Base(path)) {
					return nil
				}
				files = append(files, path)
				return nil
			})
			resultCh <- walkResult{files: files, err: walkErr}
		}(dir)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	files := append([]string{}, rootFiles...)
	for r := range resultCh {
		if r.err != nil {
			return nil, r.err
		}
		files = append(files, r.files...)
	}

	return files, nil
}

// ================== 方案三：并发 Walk + 限制 goroutine 数量 ==================
// 适合顶层子目录数量极多的场景，避免同时启动几百个 goroutine

func scanParallelLimited(rootDir string, maxWorkers int) ([]string, error) {
	entries, err := os.ReadDir(rootDir)
	if err != nil {
		return nil, fmt.Errorf("读取根目录失败: %w", err)
	}

	var topDirs []string
	var rootFiles []string

	for _, e := range entries {
		if e.IsDir() {
			topDirs = append(topDirs, filepath.Join(rootDir, e.Name()))
		} else {
			name := e.Name()
			if videoExtensions[strings.ToLower(filepath.Ext(name))] && !shouldSkip(name) {
				rootFiles = append(rootFiles, filepath.Join(rootDir, name))
			}
		}
	}

	if len(topDirs) == 0 {
		return scanSerial(rootDir)
	}

	type walkResult struct {
		files []string
		err   error
	}

	jobCh := make(chan string, len(topDirs))
	resultCh := make(chan walkResult, len(topDirs))

	var wg sync.WaitGroup
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dir := range jobCh {
				var files []string
				walkErr := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
					if err != nil {
						return nil
					}
					if info.IsDir() {
						return nil
					}
					if !videoExtensions[strings.ToLower(filepath.Ext(path))] {
						return nil
					}
					if shouldSkip(filepath.Base(path)) {
						return nil
					}
					files = append(files, path)
					return nil
				})
				resultCh <- walkResult{files: files, err: walkErr}
			}
		}()
	}

	for _, d := range topDirs {
		jobCh <- d
	}
	close(jobCh)

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	files := append([]string{}, rootFiles...)
	for r := range resultCh {
		if r.err != nil {
			return nil, r.err
		}
		files = append(files, r.files...)
	}

	return files, nil
}

// ================== Benchmark 工具函数 ==================

type BenchResult struct {
	Name     string
	Duration time.Duration
	Count    int
	Error    error
}

// runBench 执行一次扫描并计时，重复 rounds 次取平均值
func runBench(name string, rounds int, fn func() ([]string, error)) BenchResult {
	var totalDur time.Duration
	var count int
	var lastErr error

	for i := 0; i < rounds; i++ {
		start := time.Now()
		files, err := fn()
		dur := time.Since(start)
		if err != nil {
			lastErr = err
			break
		}
		totalDur += dur
		count = len(files)

		// 第一轮之后加个小间隔，让 OS 页缓存状态趋于稳定
		if i < rounds-1 {
			time.Sleep(50 * time.Millisecond)
		}
	}

	if lastErr != nil {
		return BenchResult{Name: name, Error: lastErr}
	}
	return BenchResult{
		Name:     name,
		Duration: totalDur / time.Duration(rounds),
		Count:    count,
	}
}

// ================== 目录结构分析 ==================

type DirStats struct {
	TopDirs    int
	TotalDirs  int
	TotalFiles int
	VideoFiles int
}

func analyzeDirStructure(rootDir string) DirStats {
	stats := DirStats{}

	entries, _ := os.ReadDir(rootDir)
	for _, e := range entries {
		if e.IsDir() {
			stats.TopDirs++
		}
	}

	filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			if path != rootDir {
				stats.TotalDirs++
			}
			return nil
		}
		stats.TotalFiles++
		if videoExtensions[strings.ToLower(filepath.Ext(path))] {
			stats.VideoFiles++
		}
		return nil
	})
	return stats
}

// ================== Main ==================

func main() {
	// 目标目录：优先使用命令行参数，否则使用当前 exe 所在目录
	rootDir := "."
	if exePath, err := os.Executable(); err == nil {
		rootDir = filepath.Dir(exePath)
	}
	if len(os.Args) > 1 {
		rootDir = os.Args[1]
	}

	absRoot, err := filepath.Abs(rootDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "路径解析失败: %v\n", err)
		os.Exit(1)
	}
	if _, err := os.Stat(absRoot); err != nil {
		fmt.Fprintf(os.Stderr, "目录不存在: %s\n", absRoot)
		os.Exit(1)
	}

	fmt.Printf("================================================================\n")
	fmt.Printf("  Scan Benchmark\n")
	fmt.Printf("  目标目录: %s\n", absRoot)
	fmt.Printf("  CPU 核心: %d\n", runtime.NumCPU())
	fmt.Printf("================================================================\n\n")

	// ── 目录结构分析 ──────────────────────────────────────────
	fmt.Println("[ 分析目录结构... ]")
	stats := analyzeDirStructure(absRoot)
	fmt.Printf("  顶层子目录数  : %d\n", stats.TopDirs)
	fmt.Printf("  总文件夹数    : %d\n", stats.TotalDirs)
	fmt.Printf("  总文件数      : %d\n", stats.TotalFiles)
	fmt.Printf("  视频文件数    : %d\n\n", stats.VideoFiles)

	if stats.VideoFiles == 0 {
		fmt.Println("  未找到视频文件，benchmark 无意义，退出。")
		return
	}

	// 轮数：文件少多跑几轮，文件多少跑几轮
	rounds := 5
	if stats.VideoFiles > 500 {
		rounds = 3
	}
	if stats.VideoFiles > 2000 {
		rounds = 2
	}
	fmt.Printf("  每种方案重复 %d 轮，取平均值\n\n", rounds)

	// ── 预热：先跑一次串行，让 OS 把目录元数据加载进页缓存 ──
	fmt.Println("[ 预热中（串行扫描一次，填充 OS 页缓存）... ]")
	warmupStart := time.Now()
	_, _ = scanSerial(absRoot)
	fmt.Printf("  预热耗时: %s\n\n", time.Since(warmupStart))

	// ── 定义所有测试方案 ──────────────────────────────────────
	cpuCount := runtime.NumCPU()
	workerCounts := dedupInts([]int{
		stats.TopDirs,          // 每个顶层目录一个 goroutine
		cpuCount,               // CPU 核心数
		min(stats.TopDirs, 4),  // 最多 4
		min(stats.TopDirs, 8),  // 最多 8
		min(stats.TopDirs, 16), // 最多 16
	})
	// 过滤掉 0 和重复值，只保留有意义的
	var validWorkers []int
	seen := map[int]bool{}
	for _, w := range workerCounts {
		if w > 0 && !seen[w] {
			seen[w] = true
			validWorkers = append(validWorkers, w)
		}
	}
	sort.Ints(validWorkers)

	type bench struct {
		name string
		fn   func() ([]string, error)
	}

	benches := []bench{
		{
			name: "串行 Walk（基准线）",
			fn:   func() ([]string, error) { return scanSerial(absRoot) },
		},
		{
			name: fmt.Sprintf("并发 Walk（%d goroutines，每顶层目录一个）", stats.TopDirs),
			fn:   func() ([]string, error) { return scanParallel(absRoot) },
		},
	}
	for _, w := range validWorkers {
		w := w // capture
		if w == stats.TopDirs {
			continue // 已经在上面加过了
		}
		benches = append(benches, bench{
			name: fmt.Sprintf("并发 Walk（worker pool, %d workers）", w),
			fn:   func() ([]string, error) { return scanParallelLimited(absRoot, w) },
		})
	}

	// ── 执行 Benchmark ────────────────────────────────────────
	fmt.Println("[ 开始 Benchmark ]")
	fmt.Println()

	var benchResults []BenchResult
	for _, b := range benches {
		fmt.Printf("  ▶ %s\n", b.name)
		r := runBench(b.name, rounds, b.fn)
		benchResults = append(benchResults, r)
		if r.Error != nil {
			fmt.Printf("    ✗ 错误: %v\n\n", r.Error)
		} else {
			fmt.Printf("    找到 %d 个视频文件，平均耗时: %s\n\n", r.Count, r.Duration)
		}
	}

	// ── 结果汇总 ──────────────────────────────────────────────
	fmt.Println("================================================================")
	fmt.Println("  结果汇总")
	fmt.Println("================================================================")

	// 找基准（串行）耗时
	var baselineDur time.Duration
	for _, r := range benchResults {
		if strings.Contains(r.Name, "串行") {
			baselineDur = r.Duration
			break
		}
	}

	maxNameLen := 0
	for _, r := range benchResults {
		if len(r.Name) > maxNameLen {
			maxNameLen = len(r.Name)
		}
	}

	for _, r := range benchResults {
		if r.Error != nil {
			fmt.Printf("  %-*s  ERROR: %v\n", maxNameLen, r.Name, r.Error)
			continue
		}
		speedup := ""
		if baselineDur > 0 && r.Duration > 0 && !strings.Contains(r.Name, "串行") {
			ratio := float64(baselineDur) / float64(r.Duration)
			if ratio >= 1.0 {
				speedup = fmt.Sprintf("  ← 比串行快 %.2fx", ratio)
			} else {
				speedup = fmt.Sprintf("  ← 比串行慢 %.2fx", 1.0/ratio)
			}
		}
		fmt.Printf("  %-*s  %8s  (找到 %d 个文件)%s\n",
			maxNameLen, r.Name, r.Duration.Round(time.Microsecond), r.Count, speedup)
	}

	fmt.Println()

	// ── 建议 ──────────────────────────────────────────────────
	fmt.Println("================================================================")
	fmt.Println("  分析建议")
	fmt.Println("================================================================")

	if baselineDur < 500*time.Millisecond {
		fmt.Printf("  串行扫描仅需 %s，已非常快。\n", baselineDur)
		fmt.Println("  扫描不是瓶颈，建议保持串行以降低代码复杂度。")
	} else if baselineDur < 2*time.Second {
		fmt.Printf("  串行扫描需 %s，处于可接受范围。\n", baselineDur)
		fmt.Println("  若并发方案提速明显（>1.5x）可考虑采用，否则串行即可。")
	} else {
		fmt.Printf("  串行扫描需 %s，扫描本身已成为瓶颈。\n", baselineDur)
		fmt.Println("  建议采用并发扫描方案。")
	}

	// 找最快方案
	var fastest BenchResult
	for _, r := range benchResults {
		if r.Error != nil {
			continue
		}
		if fastest.Duration == 0 || r.Duration < fastest.Duration {
			fastest = r
		}
	}
	if !strings.Contains(fastest.Name, "串行") {
		fmt.Printf("  最快方案: %s (%s)\n", fastest.Name, fastest.Duration.Round(time.Microsecond))
	}

	fmt.Println()
}

// ================== 辅助函数 ==================

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func dedupInts(s []int) []int {
	seen := map[int]bool{}
	var out []int
	for _, v := range s {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}