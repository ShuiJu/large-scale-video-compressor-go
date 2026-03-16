package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ================== 常量配置 ==================

const (
	TASK_TIMEOUT   = 4 * time.Hour
	LOG_FILE       = "transcode.log"
	SIZE_THRESHOLD = 0.80 // 转码后体积超过原文件此比例则 copy 原文件

	// 码率决策阈值（kbps）
	SKIP_BITRATE_KBP = 3500  // 低于此值：直接 copy 原文件，不编码
	HIGH_BITRATE_KBP = 10000 // 高于此值：NVEncC AV1；介于中间：SVT-AV1

	// 目标码率
	NVENC_TARGET_KBP = 6000
	SVT_TARGET_KBP   = 3000

	// I/O 并发数上下限（probe 和 copy 阶段共用）
	MIN_IO_WORKERS = 4
	MAX_IO_WORKERS = 16

	// 夜间静默窗口（本地时间，硬编码）
	// 静默区间：00:00:00 ~ 08:59:59，09:00:00 恢复
	QUIET_START_HOUR = 0 // 00:00
	QUIET_END_HOUR   = 9 // 09:00 恢复
)

// 工具路径：启动时由 checkEnvironment() 解析
var (
	NVENCC_PATH  = "NVEncC64.exe"
	FFMPEG_PATH  = "ffmpeg.exe"
	FFPROBE_PATH = "ffprobe.exe"
)

// 由常量派生的 skip 后缀，确保与目标码率常量同步
var (
	skipSuffixNVEnc = fmt.Sprintf("_av1_%dk", NVENC_TARGET_KBP)
	skipSuffixSVT   = fmt.Sprintf("_svt-av1_%dk", SVT_TARGET_KBP)
)

// ================== 数据结构 ==================

type VideoInfo struct {
	Width            int
	Height           int
	Bitrate          int  // kbps
	BitrateEstimated bool // 由文件大小/时长估算，非容器元数据
}

// TaskKind 经过 probe 分类后的任务类型
type TaskKind int

const (
	TaskCopy   TaskKind = iota // 码率低，直接 copy
	TaskNVEnc                  // 高码率，NVEncC AV1
	TaskSVTAV1                 // 中码率，SVT-AV1
)

// Task 是 probe 之后产生的带分类信息的工作单元
type Task struct {
	Path string
	Info VideoInfo
	Kind TaskKind
}

type ProcessStatus int

const (
	StatusTranscoded   ProcessStatus = iota // 成功转码，原文件入回收站
	StatusCopiedLowBR                       // 码率过低，直接 copy，原文件入回收站
	StatusCopiedTooBig                      // 转码后体积过大，copy 原文件，原文件入回收站
	StatusFailed                            // 处理失败
)

func (s ProcessStatus) String() string {
	switch s {
	case StatusTranscoded:
		return "转码成功"
	case StatusCopiedLowBR:
		return "copy原文件(码率低)"
	case StatusCopiedTooBig:
		return "copy原文件(体积未减小)"
	case StatusFailed:
		return "失败"
	}
	return "未知"
}

type Result struct {
	Path    string
	Status  ProcessStatus
	Note    string
	Elapsed time.Duration
}

// logMu 保护多 goroutine 并发写日志时的输出顺序
var (
	logMu           sync.Mutex
	resolvedLogPath = LOG_FILE // 由 initLogger 填充为完整路径
	dryRun          bool       // --dryrun 标志：只打印计划，不执行任何文件操作
)

func logf(format string, args ...interface{}) {
	logMu.Lock()
	log.Printf(format, args...)
	logMu.Unlock()
}

// ================== 初始化 ==================

func initLogger() {
	resolvedLogPath = LOG_FILE
	if exePath, err := os.Executable(); err == nil {
		resolvedLogPath = filepath.Join(filepath.Dir(exePath), LOG_FILE)
	}
	f, err := os.OpenFile(resolvedLogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("无法创建日志文件: %v", err)
	}
	log.SetOutput(io.MultiWriter(os.Stdout, f))
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

// ioWorkerCount 根据逻辑核心数决定 I/O 并发数
func ioWorkerCount() int {
	n := runtime.NumCPU()
	if n < MIN_IO_WORKERS {
		return MIN_IO_WORKERS
	}
	if n > MAX_IO_WORKERS {
		return MAX_IO_WORKERS
	}
	return n
}

// ================== 工具函数 ==================

func resolveExe(name string, searchDirs []string) string {
	for _, dir := range searchDirs {
		p := filepath.Join(dir, name)
		if _, err := os.Stat(p); err == nil {
			if abs, err := filepath.Abs(p); err == nil {
				return abs
			}
			return p
		}
	}
	if p, err := exec.LookPath(name); err == nil {
		return p
	}
	return ""
}

func checkEnvironment() {
	exeDir := "."
	if exePath, err := os.Executable(); err == nil {
		exeDir = filepath.Dir(exePath)
	}
	toolsDir := filepath.Join(exeDir, "tools")
	searchDirs := []string{exeDir, toolsDir, "."}

	log.Println("[ENV] 检查运行环境...")

	if p := resolveExe("NVEncC64.exe", searchDirs); p != "" {
		NVENCC_PATH = p
		log.Printf("[ENV] NVEncC64  : %s", NVENCC_PATH)
	} else {
		log.Fatal("[ENV] 错误：未找到 NVEncC64.exe（已搜索 exe目录、tools子目录、PATH）")
	}
	if p := resolveExe("ffmpeg.exe", searchDirs); p != "" {
		FFMPEG_PATH = p
		log.Printf("[ENV] ffmpeg    : %s", FFMPEG_PATH)
	} else {
		log.Fatal("[ENV] 错误：未找到 ffmpeg.exe")
	}
	if p := resolveExe("ffprobe.exe", searchDirs); p != "" {
		FFPROBE_PATH = p
		log.Printf("[ENV] ffprobe   : %s", FFPROBE_PATH)
	} else {
		log.Fatal("[ENV] 错误：未找到 ffprobe.exe")
	}

	log.Println("[ENV] 环境检查通过。")
}

func probeVideo(input string) (VideoInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	out, err := exec.CommandContext(ctx, FFPROBE_PATH,
		"-v", "quiet",
		"-print_format", "json",
		"-show_streams",
		"-show_format",
		input,
	).Output()
	if err != nil {
		return VideoInfo{}, fmt.Errorf("ffprobe 执行失败: %w", err)
	}

	var probe struct {
		Streams []struct {
			CodecType string `json:"codec_type"`
			Width     int    `json:"width"`
			Height    int    `json:"height"`
		} `json:"streams"`
		Format struct {
			BitRate  string `json:"bit_rate"`
			Duration string `json:"duration"`
		} `json:"format"`
	}
	if err := json.Unmarshal(out, &probe); err != nil {
		return VideoInfo{}, fmt.Errorf("ffprobe 解析失败: %w", err)
	}

	info := VideoInfo{}
	for _, s := range probe.Streams {
		if s.CodecType == "video" && s.Width > 0 {
			info.Width = s.Width
			info.Height = s.Height
			break
		}
	}
	if probe.Format.BitRate != "" {
		bps, _ := strconv.ParseInt(probe.Format.BitRate, 10, 64)
		info.Bitrate = int(bps / 1000)
	}
	// 兜底：从文件大小 + 时长估算码率（适用于 .avi/.mkv 等不含 bit_rate 字段的容器）
	if info.Bitrate == 0 && probe.Format.Duration != "" {
		dur, err := strconv.ParseFloat(probe.Format.Duration, 64)
		if err == nil && dur > 0 {
			fi, err := os.Stat(input)
			if err == nil && fi.Size() > 0 {
				info.Bitrate = int(float64(fi.Size()) * 8 / dur / 1000)
				info.BitrateEstimated = true
			}
		}
	}
	if info.Bitrate == 0 {
		return VideoInfo{}, fmt.Errorf("无法确定视频码率（容器未提供 bit_rate 且无法估算时长）")
	}
	if info.Width == 0 || info.Height == 0 {
		return VideoInfo{}, fmt.Errorf("无法读取视频分辨率")
	}
	return info, nil
}

// buildScaleFilter 生成 ffmpeg scale 滤镜（仅在超出限制时缩放，否则仅保证偶数宽高）
func buildScaleFilter(w, h int) string {
	isLandscape := w >= h
	var maxW, maxH int
	if isLandscape {
		maxW, maxH = 1920, 1080
	} else {
		maxW, maxH = 1080, 1920
	}
	if w <= maxW && h <= maxH {
		return "scale=trunc(iw/2)*2:trunc(ih/2)*2"
	}
	return fmt.Sprintf(
		"scale=w='min(iw,%d)':h='min(ih,%d)':force_original_aspect_ratio=decrease,scale=trunc(iw/2)*2:trunc(ih/2)*2",
		maxW, maxH,
	)
}

// buildNVEncScaleSize 计算 NVEncC 缩放后的目标宽高。
// 返回 (0, 0) 表示不需要缩放。
// 规则：横向 长边≤1920 短边≤1080；竖向 长边≤1920 短边≤1080，保持宽高比，宽高取偶数。
func buildNVEncScaleSize(w, h int) (newW, newH int) {
	isLandscape := w >= h
	var maxW, maxH int
	if isLandscape {
		maxW, maxH = 1920, 1080
	} else {
		maxW, maxH = 1080, 1920
	}
	if w <= maxW && h <= maxH {
		return 0, 0 // 不需要缩放
	}
	scaleW := float64(maxW) / float64(w)
	scaleH := float64(maxH) / float64(h)
	scale := scaleW
	if scaleH < scaleW {
		scale = scaleH
	}
	newW = (int(float64(w)*scale) / 2) * 2
	newH = (int(float64(h)*scale) / 2) * 2
	return newW, newH
}

// outputPath 生成最终输出文件路径（带编码后缀）
func outputPath(dir, stem, codec string, bitrate int) string {
	return filepath.Join(dir, fmt.Sprintf("%s_%s_%dk.mp4", stem, codec, bitrate))
}

func sendToRecycleBin(path string) error {
	// 路径通过环境变量注入，规避 PowerShell 字符串拼接对中文、emoji、
	// 方括号、空格等特殊字符的处理问题。
	// -LiteralPath 防止 PS 将路径中的 [ ] 当作 glob 通配符展开。
	// -EncodedCommand 传入 base64 编码的脚本，避免命令行字符集问题。
	const psScript = `
Add-Type -AssemblyName Microsoft.VisualBasic
$p = $env:RECYCLE_TARGET
[Microsoft.VisualBasic.FileIO.FileSystem]::DeleteFile(
    $p,
    [Microsoft.VisualBasic.FileIO.UIOption]::OnlyErrorDialogs,
    [Microsoft.VisualBasic.FileIO.RecycleOption]::SendToRecycleBin
)
`
	b64 := psBase64Encode(psScript)
	cmd := exec.Command("powershell",
		"-NoProfile", "-NonInteractive",
		"-EncodedCommand", b64,
	)
	cmd.Env = append(os.Environ(), "RECYCLE_TARGET="+path)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("回收站失败: %v — %s", err, string(out))
	}
	return nil
}

// psBase64Encode 将 UTF-8 字符串编码为 PowerShell -EncodedCommand 所需的 UTF-16LE+Base64 格式
func psBase64Encode(s string) string {
	runes := []rune(s)
	le := make([]byte, len(runes)*2)
	for i, r := range runes {
		le[i*2] = byte(r)
		le[i*2+1] = byte(r >> 8)
	}
	return base64.StdEncoding.EncodeToString(le)
}

func recycleOrDelete(path string) error {
	if dryRun {
		logf("  [DRYRUN] 原文件将移入回收站: %s", path)
		return nil
	}
	if err := sendToRecycleBin(path); err != nil {
		logf("  [WARN] 回收站失败，尝试直接删除: %v", err)
		if delErr := os.Remove(path); delErr != nil {
			return fmt.Errorf("删除原文件失败: %w", delErr)
		}
		logf("  原文件已直接删除（回收站不可用）")
		return nil
	}
	logf("  原文件已移入回收站")
	return nil
}

func copyFile(src, dst string) error {
	if dryRun {
		logf("  [DRYRUN] 将 copy: %s → %s", src, dst)
		return nil
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}

	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(dst)
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		os.Remove(dst)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(dst)
		return err
	}
	return nil
}

// ================== 编码实现 ==================

func runNVEnc(ctx context.Context, input, output string, info VideoInfo) error {
	if dryRun {
		logf("  [DRYRUN] NVEncC AV1 编码: %s → %s", input, output)
		return nil
	}
	// nvencRaw：NVEncC 的直接输出，未经 faststart remux
	// remuxOut：ffmpeg remux 后的结果，有 .mp4 扩展名，ffmpeg 能正确识别格式
	// output  ：由 execEncode 传入，是 finalOut+".__tmp__"，最终 rename 到 finalOut
	nvencRaw := output + ".nvenc.raw.mp4"
	remuxOut := output + ".nvenc.remux.mp4"
	defer os.Remove(nvencRaw)
	defer os.Remove(remuxOut)

	args := []string{
		"-c", "av1",
		// --level 故意不设置，让 NVEncC 根据分辨率和码率自动选择
		// 手动指定 6.1 会导致非标准分辨率（如 750x1000）触发 Invalid Level 错误
		"--preset", "quality",
		"--profile", "high",
		"-i", input,
		"-o", nvencRaw,
		"--vbr", strconv.Itoa(NVENC_TARGET_KBP),
		"--output-buf", "128",
		"--multipass", "2pass-full",
		"--lookahead", "32",
		"--bref-mode", "each",
		"--aq", "--aq-temporal",
		"--mv-precision", "Q-pel",
		"--cuda-schedule", "sync",
		"--thread-throttling", "output=on,perfmonitor=on",
		"--audio-codec", "1?aac:aac_coder=twoloop",
		"--audio-bitrate", "192",
	}
	if scaleW, scaleH := buildNVEncScaleSize(info.Width, info.Height); scaleW > 0 {
		// NVEncC 9.x 缩放语法：
		//   --output-res WxH   指定输出分辨率
		//   --vpp-resize algo=spline36   指定缩放算法
		outputRes := fmt.Sprintf("%dx%d", scaleW, scaleH)
		args = append(args,
			"--output-res", outputRes,
			"--vpp-resize", "algo=spline36",
		)
		logf("  [NVEnc] 分辨率缩放 %dx%d → %dx%d", info.Width, info.Height, scaleW, scaleH)
	}

	cmd := exec.CommandContext(ctx, NVENCC_PATH, args...)
	logf("  [NVEnc] %s", cmd.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		logf("  [NVEnc] 失败:\n%s", string(out))
		return fmt.Errorf("NVEncC: %w", err)
	}

	// faststart remux：输出必须有 .mp4 扩展名，ffmpeg 才能推断容器格式
	remux := exec.CommandContext(ctx, FFMPEG_PATH,
		"-y", "-i", nvencRaw,
		"-c", "copy",
		"-movflags", "+faststart",
		remuxOut,
	)
	logf("  [NVEnc] faststart remux...")
	if out, err := remux.CombinedOutput(); err != nil {
		logf("  [NVEnc] faststart 失败:\n%s", string(out))
		return fmt.Errorf("faststart remux: %w", err)
	}

	// remuxOut rename 到 output（即 finalOut+".__tmp__"），由 execEncode 最终 rename 到 finalOut
	if err := os.Rename(remuxOut, output); err != nil {
		return fmt.Errorf("rename remux output: %w", err)
	}
	return nil
}

func runSVTAV1(ctx context.Context, input, output string, info VideoInfo) error {
	if dryRun {
		logf("  [DRYRUN] SVT-AV1 编码: %s → %s", input, output)
		return nil
	}
	br := fmt.Sprintf("%dk", SVT_TARGET_KBP)
	scaleFilter := buildScaleFilter(info.Width, info.Height)
	passLogBase := output + ".passlog"
	defer func() {
		os.Remove(passLogBase + "-0.log")
		os.Remove(passLogBase + "-0.log.mbtree")
	}()

	pass1 := exec.CommandContext(ctx, FFMPEG_PATH,
		"-y",
		"-i", input,
		"-vf", scaleFilter,
		"-pix_fmt", "yuv420p10le",
		"-c:v", "libsvtav1",
		"-preset", "12", // Pass1 仅做码率分析，用快速 preset 加速
		"-b:v", br,
		"-pass", "1",
		"-passlogfile", passLogBase,
		"-an",
		"-f", "null", "NUL",
	)
	logf("  [SVT-AV1] Pass1: %s", pass1.String())
	if out, err := pass1.CombinedOutput(); err != nil {
		logf("  [SVT-AV1] Pass1 失败:\n%s", string(out))
		return fmt.Errorf("SVT-AV1 pass1: %w", err)
	}

	pass2 := exec.CommandContext(ctx, FFMPEG_PATH,
		"-y",
		"-i", input,
		"-vf", scaleFilter,
		"-pix_fmt", "yuv420p10le",
		"-c:v", "libsvtav1",
		"-preset", "8", // Pass2 实际编码，平衡画质与速度
		"-b:v", br,
		"-pass", "2",
		"-passlogfile", passLogBase,
		"-c:a", "aac",
		"-b:a", "192k",
		"-movflags", "+faststart",
		output,
	)
	logf("  [SVT-AV1] Pass2: %s", pass2.String())
	if out, err := pass2.CombinedOutput(); err != nil {
		logf("  [SVT-AV1] Pass2 失败:\n%s", string(out))
		return fmt.Errorf("SVT-AV1 pass2: %w", err)
	}
	return nil
}

// ================== 夜间静默 ==================

// sleepIfQuietHour 在每个文件开始处理前检查本地时间。
// 若落在 [QUIET_START_HOUR, QUIET_END_HOUR) 区间内，
// 阻塞直到当天 QUIET_END_HOUR:00:00。
// --dryrun 模式下不触发等待。
func sleepIfQuietHour() {
	if dryRun {
		return
	}
	now := time.Now()
	h := now.Hour()
	if h < QUIET_START_HOUR || h >= QUIET_END_HOUR {
		return
	}
	resume := time.Date(now.Year(), now.Month(), now.Day(),
		QUIET_END_HOUR, 0, 0, 0, now.Location())
	waitSec := int(time.Until(resume).Seconds())
	if waitSec <= 0 {
		return
	}
	logf("[SLEEP] 当前时间 %s，进入夜间静默窗口（%02d:00~%02d:00）",
		now.Format("15:04:05"), QUIET_START_HOUR, QUIET_END_HOUR)
	logf("[SLEEP] 等待 %d 秒（约 %s），将于 %02d:00:00 恢复",
		waitSec, resume.Sub(now).Round(time.Minute), QUIET_END_HOUR)
	time.Sleep(time.Until(resume))
	logf("[SLEEP] 已唤醒，继续处理。")
}

// ================== 各阶段处理函数 ==================

// execCopy 处理低码率 copy 任务（Stage 1，并发执行）
func execCopy(task Task, idx, total int) Result {
	sleepIfQuietHour()
	start := time.Now()
	res := Result{Path: task.Path}

	origStat, err := os.Stat(task.Path)
	if err != nil {
		res.Status, res.Note = StatusFailed, fmt.Sprintf("stat 失败: %v", err)
		logf("  ✗ [%d/%d] %s — %s", idx, total, filepath.Base(task.Path), res.Note)
		res.Elapsed = time.Since(start)
		return res
	}
	origSize := origStat.Size()

	dir := filepath.Dir(task.Path)
	stem := strings.TrimSuffix(filepath.Base(task.Path), filepath.Ext(task.Path))
	copyOut := filepath.Join(dir, stem+"_copy_original.mp4")

	bitrateTag := ""
	if task.Info.BitrateEstimated {
		bitrateTag = "（估算）"
	}
	logf("[%d/%d] COPY  %s  (%d kbps%s, %.2fMB)",
		idx, total, filepath.Base(task.Path), task.Info.Bitrate, bitrateTag, float64(origSize)/1024/1024)

	if err := copyFile(task.Path, copyOut); err != nil {
		res.Status, res.Note = StatusFailed, fmt.Sprintf("copy 失败: %v", err)
		logf("  ✗ %s", res.Note)
		res.Elapsed = time.Since(start)
		return res
	}
	if err := recycleOrDelete(task.Path); err != nil {
		logf("  [WARN] 原文件无法删除: %v", err)
	}
	res.Status = StatusCopiedLowBR
	res.Note = fmt.Sprintf("copy → %s，原文件已入回收站", filepath.Base(copyOut))
	logf("  ✓ %s — 耗时 %s", res.Note, time.Since(start))
	res.Elapsed = time.Since(start)
	return res
}

// execEncode 处理需要编码的任务（Stage 2/3，串行执行），包含体积判断和第二次 copy 逻辑
func execEncode(task Task, idx, total int) Result {
	sleepIfQuietHour()
	start := time.Now()
	res := Result{Path: task.Path}

	origStat, err := os.Stat(task.Path)
	if err != nil {
		res.Status, res.Note = StatusFailed, fmt.Sprintf("stat 失败: %v", err)
		logf("  ✗ %s", res.Note)
		res.Elapsed = time.Since(start)
		return res
	}
	origSize := origStat.Size()

	dir := filepath.Dir(task.Path)
	stem := strings.TrimSuffix(filepath.Base(task.Path), filepath.Ext(task.Path))

	bitrateTag := ""
	if task.Info.BitrateEstimated {
		bitrateTag = "（估算）"
	}
	var codec string
	var targetKB int
	switch task.Kind {
	case TaskNVEnc:
		codec, targetKB = "av1", NVENC_TARGET_KBP
		logf("[%d/%d] NVENC  %s  (%d kbps%s → %d kbps, %.2fMB)",
			idx, total, filepath.Base(task.Path), task.Info.Bitrate, bitrateTag, targetKB, float64(origSize)/1024/1024)
	case TaskSVTAV1:
		codec, targetKB = "svt-av1", SVT_TARGET_KBP
		logf("[%d/%d] SVT-AV1  %s  (%d kbps%s → %d kbps, %.2fMB)",
			idx, total, filepath.Base(task.Path), task.Info.Bitrate, bitrateTag, targetKB, float64(origSize)/1024/1024)
	}

	finalOut := outputPath(dir, stem, codec, targetKB)
	tmpOut := finalOut + ".__tmp__"
	defer os.Remove(tmpOut)

	ctx, cancel := context.WithTimeout(context.Background(), TASK_TIMEOUT)
	defer cancel()

	var encErr error
	switch task.Kind {
	case TaskNVEnc:
		encErr = runNVEnc(ctx, task.Path, tmpOut, task.Info)
	case TaskSVTAV1:
		encErr = runSVTAV1(ctx, task.Path, tmpOut, task.Info)
	}

	if encErr != nil {
		res.Status, res.Note = StatusFailed, fmt.Sprintf("编码失败: %v", encErr)
		logf("  ✗ %s", res.Note)
		os.Remove(tmpOut)
		res.Elapsed = time.Since(start)
		return res
	}

	newStat, err := os.Stat(tmpOut)
	if err != nil {
		res.Status, res.Note = StatusFailed, "编码输出文件不存在"
		logf("  ✗ %s", res.Note)
		res.Elapsed = time.Since(start)
		return res
	}
	newSize := newStat.Size()
	ratio := float64(newSize) / float64(origSize)
	logf("  体积对比: %.2fMB → %.2fMB (%.1f%%)",
		float64(origSize)/1024/1024, float64(newSize)/1024/1024, ratio*100)

	// 第二次判断：体积超标则改为 copy 原文件
	if ratio > SIZE_THRESHOLD {
		logf("  转码后体积 %.1f%% > %.0f%% 阈值，改为 copy 原文件", ratio*100, SIZE_THRESHOLD*100)
		os.Remove(tmpOut)
		if err := copyFile(task.Path, finalOut); err != nil {
			res.Status, res.Note = StatusFailed, fmt.Sprintf("copy 原文件失败: %v", err)
			logf("  ✗ %s", res.Note)
			res.Elapsed = time.Since(start)
			return res
		}
		res.Status = StatusCopiedTooBig
		res.Note = fmt.Sprintf("%.1f%% > %.0f%% 阈值，copy 原文件 → %s",
			ratio*100, SIZE_THRESHOLD*100, filepath.Base(finalOut))
	} else {
		if err := os.Rename(tmpOut, finalOut); err != nil {
			res.Status, res.Note = StatusFailed, fmt.Sprintf("重命名输出文件失败: %v", err)
			logf("  ✗ %s", res.Note)
			res.Elapsed = time.Since(start)
			return res
		}
		res.Status = StatusTranscoded
		res.Note = fmt.Sprintf("%.2fMB → %.2fMB (%.1f%%) → %s",
			float64(origSize)/1024/1024, float64(newSize)/1024/1024, ratio*100, filepath.Base(finalOut))
	}

	if err := recycleOrDelete(task.Path); err != nil {
		logf("  [WARN] 原文件无法删除: %v", err)
	}

	logf("  ✓ [%s] %s — 耗时 %s", res.Status, res.Note, time.Since(start))
	res.Elapsed = time.Since(start)
	return res
}

// ================== 扫描 ==================

var videoExtensions = map[string]bool{
	".mp4": true,
	".mov": true,
	".avi": true,
	".mkv": true,
	".wmv": true,
	".m4v": true,
}

func scanVideos(rootDir string) ([]string, error) {
	var files []string
	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			logf("[SCAN] 访问失败: %s — %v", path, err)
			return nil
		}
		if info.IsDir() {
			return nil
		}
		if !videoExtensions[strings.ToLower(filepath.Ext(path))] {
			return nil
		}
		base := filepath.Base(path)
		stem := strings.TrimSuffix(base, filepath.Ext(base))
		// 跳过本程序生成的临时文件和已处理的输出文件
		if strings.Contains(base, ".__tmp__") ||
			strings.HasSuffix(stem, skipSuffixNVEnc) ||
			strings.HasSuffix(stem, skipSuffixSVT) ||
			strings.HasSuffix(stem, "_copy_original") {
			logf("[SCAN] 跳过已处理文件: %s", path)
			return nil
		}
		files = append(files, path)
		return nil
	})
	return files, err
}

// ================== 并发 Probe ==================

type probeResult struct {
	path string
	info VideoInfo
	err  error
}

// probeAll 并发对所有文件执行 ffprobe，返回分类后的任务列表和失败列表
func probeAll(files []string) (tasks []Task, failed []Result) {
	workers := ioWorkerCount()
	logf("[PROBE] 并发 probe %d 个文件（%d workers）...", len(files), workers)

	jobCh := make(chan string, len(files))
	resultCh := make(chan probeResult, len(files))

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range jobCh {
				info, err := probeVideo(path)
				resultCh <- probeResult{path: path, info: info, err: err}
			}
		}()
	}

	for _, f := range files {
		jobCh <- f
	}
	close(jobCh)

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	done := 0
	total := len(files)
	kindNames := []string{"COPY", "NVENC-AV1", "SVT-AV1"}

	for r := range resultCh {
		done++
		if r.err != nil {
			logf("[PROBE] ✗ [%d/%d] %s — %v", done, total, filepath.Base(r.path), r.err)
			failed = append(failed, Result{
				Path:   r.path,
				Status: StatusFailed,
				Note:   fmt.Sprintf("probe 失败: %v", r.err),
			})
			continue
		}

		var kind TaskKind
		switch {
		case r.info.Bitrate < SKIP_BITRATE_KBP:
			kind = TaskCopy
		case r.info.Bitrate > HIGH_BITRATE_KBP:
			kind = TaskNVEnc
		default:
			kind = TaskSVTAV1
		}

		bitrateTag := ""
		if r.info.BitrateEstimated {
			bitrateTag = "（估算）"
		}
		logf("[PROBE] ✓ [%d/%d] %s — %dx%d %d kbps%s → %s",
			done, total, filepath.Base(r.path),
			r.info.Width, r.info.Height, r.info.Bitrate, bitrateTag,
			kindNames[kind])

		tasks = append(tasks, Task{Path: r.path, Info: r.info, Kind: kind})
	}
	return tasks, failed
}

// ================== Main ==================

func main() {
	// ── 命令行参数解析 ────────────────────────────────────────
	flag.BoolVar(&dryRun, "dryrun", false, "只扫描和分析，打印完整执行计划，不执行任何文件操作")
	flag.Parse()

	initLogger()
	checkEnvironment()

	rootDir := "."
	if exePath, err := os.Executable(); err == nil {
		rootDir = filepath.Dir(exePath)
	}
	logf("[MAIN] 根目录: %s", rootDir)
	if dryRun {
		logf("[MAIN] *** DRYRUN 模式：只打印计划，不执行任何文件操作 ***")
	}

	// ── Step 1：扫描 ──────────────────────────────────────────
	logf("[SCAN] 开始扫描视频文件...")
	files, err := scanVideos(rootDir)
	if err != nil {
		log.Fatalf("[SCAN] 扫描失败: %v", err)
	}
	logf("[SCAN] 共找到 %d 个视频文件", len(files))
	if len(files) == 0 {
		logf("[MAIN] 没有需要处理的文件，退出。")
		return
	}

	// ── Step 2：并发 Probe + 分类 ─────────────────────────────
	tasks, probeFailures := probeAll(files)

	var copyTasks, nvencTasks, svtTasks []Task
	for _, t := range tasks {
		switch t.Kind {
		case TaskCopy:
			copyTasks = append(copyTasks, t)
		case TaskNVEnc:
			nvencTasks = append(nvencTasks, t)
		case TaskSVTAV1:
			svtTasks = append(svtTasks, t)
		}
	}

	logf("\n[PLAN] 任务清单:")
	logf("       阶段1 COPY    : %d 个（码率 < %dkbps，并发执行）", len(copyTasks), SKIP_BITRATE_KBP)
	logf("       阶段2 NVENC   : %d 个（码率 > %dkbps，串行执行）", len(nvencTasks), HIGH_BITRATE_KBP)
	logf("       阶段3 SVT-AV1 : %d 个（码率 %d~%dkbps，串行执行）", len(svtTasks), SKIP_BITRATE_KBP, HIGH_BITRATE_KBP)
	logf("       Probe 失败    : %d 个", len(probeFailures))

	// ── Dryrun：打印完整计划后退出 ────────────────────────────
	if dryRun {
		logf("\n[DRYRUN] ══════ 阶段1：COPY（%d 个）══════", len(copyTasks))
		for i, t := range copyTasks {
			fi, _ := os.Stat(t.Path)
			var sizeMB float64
			if fi != nil { sizeMB = float64(fi.Size()) / 1024 / 1024 }
			dir := filepath.Dir(t.Path)
			stem := strings.TrimSuffix(filepath.Base(t.Path), filepath.Ext(t.Path))
			dst := filepath.Join(dir, stem+"_copy_original.mp4")
			bitrateTag := ""
			if t.Info.BitrateEstimated { bitrateTag = "（估算）" }
			logf("[DRYRUN] COPY  [%d/%d] %s", i+1, len(copyTasks), t.Path)
			logf("               码率: %d kbps%s  大小: %.2fMB", t.Info.Bitrate, bitrateTag, sizeMB)
			logf("               输出: %s", dst)
			logf("               原文件: → 回收站")
		}

		logf("\n[DRYRUN] ══════ 阶段2：NVEncC AV1（%d 个）══════", len(nvencTasks))
		for i, t := range nvencTasks {
			fi, _ := os.Stat(t.Path)
			var sizeMB float64
			if fi != nil { sizeMB = float64(fi.Size()) / 1024 / 1024 }
			dir := filepath.Dir(t.Path)
			stem := strings.TrimSuffix(filepath.Base(t.Path), filepath.Ext(t.Path))
			dst := outputPath(dir, stem, "av1", NVENC_TARGET_KBP)
			bitrateTag := ""
			if t.Info.BitrateEstimated { bitrateTag = "（估算）" }
			logf("[DRYRUN] NVENC [%d/%d] %s", i+1, len(nvencTasks), t.Path)
			logf("               码率: %d kbps%s → %d kbps  大小: %.2fMB", t.Info.Bitrate, bitrateTag, NVENC_TARGET_KBP, sizeMB)
			logf("               输出: %s", dst)
			logf("               原文件: → 回收站")
		}

		logf("\n[DRYRUN] ══════ 阶段3：SVT-AV1（%d 个）══════", len(svtTasks))
		for i, t := range svtTasks {
			fi, _ := os.Stat(t.Path)
			var sizeMB float64
			if fi != nil { sizeMB = float64(fi.Size()) / 1024 / 1024 }
			dir := filepath.Dir(t.Path)
			stem := strings.TrimSuffix(filepath.Base(t.Path), filepath.Ext(t.Path))
			dst := outputPath(dir, stem, "svt-av1", SVT_TARGET_KBP)
			bitrateTag := ""
			if t.Info.BitrateEstimated { bitrateTag = "（估算）" }
			logf("[DRYRUN] SVT   [%d/%d] %s", i+1, len(svtTasks), t.Path)
			logf("               码率: %d kbps%s → %d kbps  大小: %.2fMB", t.Info.Bitrate, bitrateTag, SVT_TARGET_KBP, sizeMB)
			logf("               输出: %s", dst)
			logf("               原文件: → 回收站")
		}

		if len(probeFailures) > 0 {
			logf("\n[DRYRUN] ══════ Probe 失败（%d 个，将跳过）══════", len(probeFailures))
			for _, r := range probeFailures {
				logf("[DRYRUN] SKIP  %s — %s", r.Path, r.Note)
			}
		}

		logf("\n[DRYRUN] 以上为完整执行计划，未执行任何实际操作。")
		logf("[DRYRUN] 去掉 --dryrun 参数后正式运行。")
		return
	}

	results := make([]Result, 0, len(files))
	results = append(results, probeFailures...)
	logf("")

	// ── Step 3a：并发 Copy ────────────────────────────────────
	if len(copyTasks) > 0 {
		workers := ioWorkerCount()
		logf("[STAGE 1/3] 并发 copy %d 个低码率文件（%d workers）...", len(copyTasks), workers)

		type indexedTask struct {
			task Task
			idx  int
		}
		jobCh := make(chan indexedTask, len(copyTasks))
		resCh := make(chan Result, len(copyTasks))

		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for jt := range jobCh {
					resCh <- execCopy(jt.task, jt.idx, len(copyTasks))
				}
			}()
		}
		for i, t := range copyTasks {
			jobCh <- indexedTask{task: t, idx: i + 1}
		}
		close(jobCh)
		go func() {
			wg.Wait()
			close(resCh)
		}()
		for r := range resCh {
			results = append(results, r)
		}
		logf("[STAGE 1/3] Copy 阶段完成。")
	}

	// ── Step 3b：串行 NVEncC AV1 ─────────────────────────────
	if len(nvencTasks) > 0 {
		logf("[STAGE 2/3] NVEncC AV1 编码 %d 个文件（串行）...", len(nvencTasks))
		for i, task := range nvencTasks {
			results = append(results, execEncode(task, i+1, len(nvencTasks)))
		}
		logf("[STAGE 2/3] NVEncC 阶段完成。")
	}

	// ── Step 3c：串行 SVT-AV1 ────────────────────────────────
	if len(svtTasks) > 0 {
		logf("[STAGE 3/3] SVT-AV1 编码 %d 个文件（串行）...", len(svtTasks))
		for i, task := range svtTasks {
			results = append(results, execEncode(task, i+1, len(svtTasks)))
		}
		logf("[STAGE 3/3] SVT-AV1 阶段完成。")
	}

	// ── 汇总 ─────────────────────────────────────────────────
	var cntTranscoded, cntCopiedLow, cntCopiedBig, cntFailed int
	for _, r := range results {
		switch r.Status {
		case StatusTranscoded:
			cntTranscoded++
		case StatusCopiedLowBR:
			cntCopiedLow++
		case StatusCopiedTooBig:
			cntCopiedBig++
		case StatusFailed:
			cntFailed++
		}
	}

	logf("\n╔══════════════ 处理完成 ══════════════╗")
	logf("║ 成功转码                : %d", cntTranscoded)
	logf("║ copy 原文件 (码率过低)  : %d  (< %dkbps，原文件入回收站)", cntCopiedLow, SKIP_BITRATE_KBP)
	logf("║ copy 原文件 (体积未减小): %d  (转码后 > %.0f%%，原文件入回收站)", cntCopiedBig, SIZE_THRESHOLD*100)
	logf("║ 失败                    : %d", cntFailed)
	logf("║ 合计                    : %d", len(files))
	logf("║ 日志文件                : %s", resolvedLogPath)
	logf("╚══════════════════════════════════════╝")
}