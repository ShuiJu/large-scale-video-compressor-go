package main

import (
	"bufio"
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
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ================== 常量配置 ==================

const (
	TASK_TIMEOUT   = 4 * time.Hour
	LOG_FILE       = "transcode.log"
	SIZE_THRESHOLD = 0.80 // 转码后体积超过原文件此比例则体积不达标

	// 码率决策阈值（kbps）
	SKIP_BITRATE_KBP = 3500  // 低于此值：直接 copy 原文件，不编码
	HIGH_BITRATE_KBP = 10000 // 高于此值：NVEncC AV1；介于中间：SVT-AV1

	// 目标码率
	NVENC_TARGET_KBP     = 6000 // NVEncC AV1 目标码率
	SVT_TARGET_KBP       = 3000 // SVT-AV1 标准目标码率
	SVT_RETRY_KBP        = 4000 // NVEnc 不达标时 SVT 重试码率
	SVT_P6_THRESHOLD_KBP = 4000 // SVT 结果码率超过此值时触发 preset6 重试

	// I/O 并发数上下限（probe 和 copy 阶段共用）
	MIN_IO_WORKERS = 4
	MAX_IO_WORKERS = 16

	// 夜间静默窗口（本地时间，硬编码）
	QUIET_START_HOUR = 0 // 00:00
	QUIET_END_HOUR   = 9 // 09:00 恢复

	// 最多完整执行轮数
	MAX_ROUNDS = 2
)

// 工具路径：启动时由 checkEnvironment() 解析
var (
	NVENCC_PATH  = "NVEncC64.exe"
	FFMPEG_PATH  = "ffmpeg.exe"
	FFPROBE_PATH = "ffprobe.exe"
)

// 由常量派生的跳过后缀（已完成处理）
var (
	skipSuffixNVEnc        = fmt.Sprintf("_av1_%dk", NVENC_TARGET_KBP)
	skipSuffixSVT          = fmt.Sprintf("_svt-av1_%dk", SVT_TARGET_KBP)
	skipSuffixSVT4000      = fmt.Sprintf("_svt-av1_%dk", SVT_RETRY_KBP)
	skipSuffixSVTP6        = fmt.Sprintf("_svt-av1_p6_%dk", SVT_TARGET_KBP)
	skipSuffixItDoesntWork = "_it_doesnt_work"
	skipSuffixCopyOrig     = "_copy_original"

	// 直接入队后缀（无需 probe，TaskKind 已知）
	directSuffixNeedSVT4000 = "_need_svt_4000"
	directSuffixNeedSVTP6   = "_need_svt_p6"
)

// ================== 数据结构 ==================

type VideoInfo struct {
	Width            int
	Height           int
	Bitrate          int    // kbps
	BitrateEstimated bool   // 由文件大小/时长估算，非容器元数据
	CodecName        string // 视频流编码格式（如 h264, hevc, mpeg4 …）
}

// TaskKind 经过 probe 分类后的任务类型
type TaskKind int

const (
	TaskCopy        TaskKind = iota // 码率低，直接 copy
	TaskNVEnc                       // 高码率，NVEncC AV1
	TaskSVTAV1                      // 中码率，SVT-AV1 3000k preset8
	TaskNeedSVT4000                 // _need_svt_4000 → SVT-AV1 4000k preset8 重试
	TaskNeedSVTP6                   // _need_svt_p6   → SVT-AV1 3000k preset6 重试
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
	StatusCopiedTooBig                      // SVT 体积不达标（低码率通路），copy 原文件
	StatusNeedSVT4000                       // NVEnc 体积不达标，原文件改名为 _need_svt_4000
	StatusNeedSVTP6                         // SVT 体积不达标（高码率），原文件改名为 _need_svt_p6
	StatusItDoesntWork                      // 重试仍体积不达标，原文件改名为 _it_doesnt_work
	StatusFailed                            // 处理失败（编码报错）
)

func (s ProcessStatus) String() string {
	switch s {
	case StatusTranscoded:
		return "转码成功"
	case StatusCopiedLowBR:
		return "copy原文件(码率低)"
	case StatusCopiedTooBig:
		return "copy原文件(体积未减小)"
	case StatusNeedSVT4000:
		return "待重试(need_svt_4000)"
	case StatusNeedSVTP6:
		return "待重试(need_svt_p6)"
	case StatusItDoesntWork:
		return "it_doesnt_work"
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
	checkLogMode    bool       // --checklog 标志：只分析日志并生成报告
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
			CodecName string `json:"codec_name"`
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
			info.CodecName = s.CodecName
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

// stripInputSuffix 从 stem 末尾去掉 suffix（不含扩展名部分）
func stripInputSuffix(stem, suffix string) string {
	if strings.HasSuffix(stem, suffix) {
		return stem[:len(stem)-len(suffix)]
	}
	return stem
}

// probeFileBitrate 用 ffprobe 读取已编码文件的码率（kbps），失败返回 0
func probeFileBitrate(path string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, FFPROBE_PATH,
		"-v", "quiet", "-print_format", "json", "-show_format", path,
	).Output()
	if err != nil {
		return 0
	}
	var probe struct {
		Format struct {
			BitRate  string `json:"bit_rate"`
			Duration string `json:"duration"`
		} `json:"format"`
	}
	if err := json.Unmarshal(out, &probe); err != nil {
		return 0
	}
	if probe.Format.BitRate != "" {
		bps, _ := strconv.ParseInt(probe.Format.BitRate, 10, 64)
		return int(bps / 1000)
	}
	return 0
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
	// NVDEC 不支持 MPEG4 Part2 / VP8 等格式，遇到则强制软解避免 cuvidCreateDecoder 失败
	nvdecSupported := map[string]bool{
		"h264": true, "hevc": true, "av1": true, "vp9": true, "mpeg2video": true,
	}
	if !nvdecSupported[info.CodecName] {
		args = append(args, "--avsw")
		logf("  [NVEnc] 输入编码 %q 不支持 NVDEC，使用软解 (--avsw)", info.CodecName)
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

// runSVT 执行 SVT-AV1 两遍编码。
// Pass1 固定 preset12（仅码率分析）；Pass2 使用 pass2Preset 和 targetKbps。
func runSVT(ctx context.Context, input, output string, info VideoInfo, targetKbps, pass2Preset int) error {
	if dryRun {
		logf("  [DRYRUN] SVT-AV1 编码: %s → %s (br=%dk, preset=%d)", input, output, targetKbps, pass2Preset)
		return nil
	}
	br := fmt.Sprintf("%dk", targetKbps)
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
		"-preset", "12",
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
		"-preset", strconv.Itoa(pass2Preset),
		"-b:v", br,
		"-pass", "2",
		"-passlogfile", passLogBase,
		"-c:a", "aac",
		"-b:a", "192k",
		"-movflags", "+faststart",
		"-f", "mp4", // output 以 .__tmp__ 结尾，ffmpeg 无法从扩展名推断格式
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

// execEncode 处理需要编码的任务（串行执行），包含体积判断和后续决策逻辑
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
	inputStem := strings.TrimSuffix(filepath.Base(task.Path), filepath.Ext(task.Path))

	bitrateTag := ""
	if task.Info.BitrateEstimated {
		bitrateTag = "（估算）"
	}

	// 根据 TaskKind 决定编码参数和输出命名
	var outputStem, encoderTag, codec string
	var targetKbps int
	pass2Preset := 8 // SVT 默认 preset，NVEnc 不使用此值
	switch task.Kind {
	case TaskNVEnc:
		outputStem = inputStem
		encoderTag = "NVENC"
		codec = "av1"
		targetKbps = NVENC_TARGET_KBP
	case TaskSVTAV1:
		outputStem = inputStem
		encoderTag = "SVT-AV1"
		codec = "svt-av1"
		targetKbps = SVT_TARGET_KBP
		pass2Preset = 8
	case TaskNeedSVT4000:
		outputStem = stripInputSuffix(inputStem, directSuffixNeedSVT4000)
		encoderTag = "SVT-AV1(retry4000)"
		codec = "svt-av1"
		targetKbps = SVT_RETRY_KBP
		pass2Preset = 8
	case TaskNeedSVTP6:
		outputStem = stripInputSuffix(inputStem, directSuffixNeedSVTP6)
		encoderTag = "SVT-AV1(retryP6)"
		codec = "svt-av1_p6"
		targetKbps = SVT_TARGET_KBP
		pass2Preset = 6
	}

	logf("[%d/%d] %s  %s  (%d kbps%s → %d kbps, %.2fMB)",
		idx, total, encoderTag, filepath.Base(task.Path),
		task.Info.Bitrate, bitrateTag, targetKbps, float64(origSize)/1024/1024)
	// 结构化行供 --checklog 解析（path 最后，支持含空格的路径）
	logf("[ENCODE] encoder=%s origSizeMB=%.2f origBR=%d path=%s",
		strings.ToLower(encoderTag), float64(origSize)/1024/1024, task.Info.Bitrate, task.Path)

	finalOut := outputPath(dir, outputStem, codec, targetKbps)
	tmpOut := finalOut + ".__tmp__"
	defer os.Remove(tmpOut)

	ctx, cancel := context.WithTimeout(context.Background(), TASK_TIMEOUT)
	defer cancel()

	var encErr error
	if task.Kind == TaskNVEnc {
		encErr = runNVEnc(ctx, task.Path, tmpOut, task.Info)
	} else {
		encErr = runSVT(ctx, task.Path, tmpOut, task.Info, targetKbps, pass2Preset)
	}

	if encErr != nil {
		res.Status, res.Note = StatusFailed, fmt.Sprintf("编码失败: %v", encErr)
		logf("  ✗ %s", res.Note)
		logf("[RESULT] status=failed")
		os.Remove(tmpOut)
		res.Elapsed = time.Since(start)
		return res
	}

	newStat, err := os.Stat(tmpOut)
	if err != nil {
		res.Status, res.Note = StatusFailed, "编码输出文件不存在"
		logf("  ✗ %s", res.Note)
		logf("[RESULT] status=failed")
		res.Elapsed = time.Since(start)
		return res
	}
	newSize := newStat.Size()
	ratio := float64(newSize) / float64(origSize)
	logf("  体积对比: %.2fMB → %.2fMB (%.1f%%)",
		float64(origSize)/1024/1024, float64(newSize)/1024/1024, ratio*100)

	if ratio > SIZE_THRESHOLD {
		logf("  转码后体积 %.1f%% > %.0f%% 阈值", ratio*100, SIZE_THRESHOLD*100)

		switch task.Kind {
		case TaskNVEnc:
			// NVEnc 不达标：删除编码结果，原文件改名为 _need_svt_4000
			os.Remove(tmpOut)
			newName := filepath.Join(dir, outputStem+directSuffixNeedSVT4000+".mp4")
			if err := os.Rename(task.Path, newName); err != nil {
				res.Status, res.Note = StatusFailed, fmt.Sprintf("改名 _need_svt_4000 失败: %v", err)
				logf("  ✗ %s", res.Note)
				logf("[RESULT] status=failed")
				res.Elapsed = time.Since(start)
				return res
			}
			res.Status = StatusNeedSVT4000
			res.Note = fmt.Sprintf("NVEnc 体积不达标，改名 → %s", filepath.Base(newName))
			logf("  ↻ %s", res.Note)
			logf("[RESULT] status=need_svt_4000")

		case TaskSVTAV1:
			// SVT 3000k 不达标：先 probe 编码结果码率，再决策
			encodedBR := probeFileBitrate(tmpOut)
			os.Remove(tmpOut)
			if encodedBR > SVT_P6_THRESHOLD_KBP {
				// 编码结果码率仍高 → 改名为 _need_svt_p6，用 preset6 重试
				newName := filepath.Join(dir, outputStem+directSuffixNeedSVTP6+".mp4")
				if err := os.Rename(task.Path, newName); err != nil {
					res.Status, res.Note = StatusFailed, fmt.Sprintf("改名 _need_svt_p6 失败: %v", err)
					logf("  ✗ %s", res.Note)
					logf("[RESULT] status=failed")
					res.Elapsed = time.Since(start)
					return res
				}
				res.Status = StatusNeedSVTP6
				res.Note = fmt.Sprintf("SVT 体积不达标（结果 %dkbps > %dkbps），改名 → %s",
					encodedBR, SVT_P6_THRESHOLD_KBP, filepath.Base(newName))
				logf("  ↻ %s", res.Note)
				logf("[RESULT] status=need_svt_p6")
			} else {
				// 编码结果码率已足够低，重试无意义 → copy 原文件
				if err := copyFile(task.Path, finalOut); err != nil {
					res.Status, res.Note = StatusFailed, fmt.Sprintf("copy 原文件失败: %v", err)
					logf("  ✗ %s", res.Note)
					logf("[RESULT] status=failed")
					res.Elapsed = time.Since(start)
					return res
				}
				res.Status = StatusCopiedTooBig
				res.Note = fmt.Sprintf("体积不达标（结果码率 %dkbps ≤ %dkbps，无需重试），copy → %s",
					encodedBR, SVT_P6_THRESHOLD_KBP, filepath.Base(finalOut))
				logf("  → %s", res.Note)
				logf("[RESULT] status=copied_too_big")
			}

		case TaskNeedSVT4000, TaskNeedSVTP6:
			// 重试仍不达标 → 原文件改名为 _it_doesnt_work（终止）
			os.Remove(tmpOut)
			newName := filepath.Join(dir, outputStem+"_it_doesnt_work.mp4")
			if err := os.Rename(task.Path, newName); err != nil {
				res.Status, res.Note = StatusFailed, fmt.Sprintf("改名 _it_doesnt_work 失败: %v", err)
				logf("  ✗ %s", res.Note)
				logf("[RESULT] status=failed")
				res.Elapsed = time.Since(start)
				return res
			}
			res.Status = StatusItDoesntWork
			res.Note = fmt.Sprintf("重试仍体积不达标，改名 → %s", filepath.Base(newName))
			logf("  ✗ %s", res.Note)
			logf("[RESULT] status=it_doesnt_work")
		}

		// 对于改名操作（不 copy），原文件已被 os.Rename，无需额外回收
		// 对于 copy 操作（StatusCopiedTooBig），原文件入回收站
		if res.Status == StatusCopiedTooBig {
			if err := recycleOrDelete(task.Path); err != nil {
				logf("  [WARN] 原文件无法删除: %v", err)
			}
		}
	} else {
		// 体积达标：保留编码结果
		if err := os.Rename(tmpOut, finalOut); err != nil {
			res.Status, res.Note = StatusFailed, fmt.Sprintf("重命名输出文件失败: %v", err)
			logf("  ✗ %s", res.Note)
			logf("[RESULT] status=failed")
			res.Elapsed = time.Since(start)
			return res
		}
		res.Status = StatusTranscoded
		res.Note = fmt.Sprintf("%.2fMB → %.2fMB (%.1f%%) → %s",
			float64(origSize)/1024/1024, float64(newSize)/1024/1024, ratio*100, filepath.Base(finalOut))
		logf("  ✓ [转码成功] %s", res.Note)
		logf("[RESULT] status=transcoded")

		if err := recycleOrDelete(task.Path); err != nil {
			logf("  [WARN] 原文件无法删除: %v", err)
		}
	}

	logf("  耗时 %s", time.Since(start))
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

// scannedFile 代表一个已确定 TaskKind 的文件（直接入队，无需 probe 分类）
type scannedFile struct {
	path string
	kind TaskKind
}

// scanVideos 递归扫描 rootDir，返回：
//   - direct：已知 TaskKind 的文件（_need_svt_4000 / _need_svt_p6）
//   - toProbe：需要 ffprobe 决定 TaskKind 的文件
func scanVideos(rootDir string) (direct []scannedFile, toProbe []string, err error) {
	err = filepath.Walk(rootDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			logf("[SCAN] 访问失败: %s — %v", path, walkErr)
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

		// 跳过：含 .__tmp__ 或已完成处理的后缀
		if strings.Contains(base, ".__tmp__") ||
			strings.HasSuffix(stem, skipSuffixNVEnc) ||
			strings.HasSuffix(stem, skipSuffixSVT) ||
			strings.HasSuffix(stem, skipSuffixSVT4000) ||
			strings.HasSuffix(stem, skipSuffixSVTP6) ||
			strings.HasSuffix(stem, skipSuffixItDoesntWork) ||
			strings.HasSuffix(stem, skipSuffixCopyOrig) {
			logf("[SCAN] 跳过已处理文件: %s", path)
			return nil
		}

		// 直接入队：_need_svt_4000 / _need_svt_p6
		if strings.HasSuffix(stem, directSuffixNeedSVT4000) {
			direct = append(direct, scannedFile{path: path, kind: TaskNeedSVT4000})
			return nil
		}
		if strings.HasSuffix(stem, directSuffixNeedSVTP6) {
			direct = append(direct, scannedFile{path: path, kind: TaskNeedSVTP6})
			return nil
		}

		toProbe = append(toProbe, path)
		return nil
	})
	return direct, toProbe, err
}

// ================== 并发 Probe ==================

type probeResult struct {
	path    string
	preKind TaskKind // 非零表示 Kind 已预设（直接入队文件）
	info    VideoInfo
	err     error
}

// probeAll 并发对所有文件执行 ffprobe。
//   - toProbe：按码率决定 TaskKind
//   - direct：TaskKind 已预设，仅 probe 获取 VideoInfo 供编码使用
func probeAll(toProbe []string, direct []scannedFile) (tasks []Task, failed []Result) {
	type job struct {
		path    string
		preKind TaskKind
	}
	var jobs []job
	for _, f := range toProbe {
		jobs = append(jobs, job{path: f, preKind: 0})
	}
	for _, d := range direct {
		jobs = append(jobs, job{path: d.path, preKind: d.kind})
	}

	if len(jobs) == 0 {
		return nil, nil
	}

	workers := ioWorkerCount()
	logf("[PROBE] 并发 probe %d 个文件（%d workers）...", len(jobs), workers)

	jobCh := make(chan job, len(jobs))
	resultCh := make(chan probeResult, len(jobs))

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobCh {
				info, err := probeVideo(j.path)
				resultCh <- probeResult{path: j.path, preKind: j.preKind, info: info, err: err}
			}
		}()
	}
	for _, j := range jobs {
		jobCh <- j
	}
	close(jobCh)
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	done := 0
	total := len(jobs)
	kindNames := map[TaskKind]string{
		TaskCopy:        "COPY",
		TaskNVEnc:       "NVENC-AV1",
		TaskSVTAV1:      "SVT-AV1",
		TaskNeedSVT4000: "SVT-AV1(retry4000)",
		TaskNeedSVTP6:   "SVT-AV1(retryP6)",
	}

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
		if r.preKind != 0 {
			// 直接入队：使用预设 Kind
			kind = r.preKind
		} else {
			// 按码率分类
			switch {
			case r.info.Bitrate < SKIP_BITRATE_KBP:
				kind = TaskCopy
			case r.info.Bitrate > HIGH_BITRATE_KBP:
				kind = TaskNVEnc
			default:
				kind = TaskSVTAV1
			}
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
	flag.BoolVar(&dryRun, "dryrun", false, "只扫描和分析，打印完整执行计划，不执行任何文件操作")
	flag.BoolVar(&checkLogMode, "checklog", false, "分析 transcode.log 并生成报告，不执行任何编码")
	flag.Parse()

	initLogger()

	if checkLogMode {
		reportDir := "."
		if exePath, err := os.Executable(); err == nil {
			reportDir = filepath.Dir(exePath)
		}
		runCheckLog(resolvedLogPath, reportDir)
		return
	}

	checkEnvironment()

	rootDir := "."
	if exePath, err := os.Executable(); err == nil {
		rootDir = filepath.Dir(exePath)
	}
	logf("[MAIN] 根目录: %s", rootDir)
	if dryRun {
		logf("[MAIN] *** DRYRUN 模式：只打印计划，不执行任何文件操作 ***")
	}

	for round := 1; round <= MAX_ROUNDS; round++ {
		logf("[MAIN] ════════ 第 %d/%d 轮开始 ════════", round, MAX_ROUNDS)
		hasRetry := runOnce(rootDir)

		reportDir := rootDir
		runCheckLog(resolvedLogPath, reportDir)

		if !hasRetry || round == MAX_ROUNDS {
			if round == MAX_ROUNDS && hasRetry {
				logf("[MAIN] 已达最大轮数 %d，尚有待重试文件，下次运行程序时继续处理。", MAX_ROUNDS)
			}
			break
		}
		logf("[MAIN] 第 %d 轮产生了待重试文件，开始第 %d 轮。", round, round+1)
	}

	logf("[MAIN] 全部处理完成，程序退出。")
}

// runOnce 执行一次完整的扫描+编码流程，返回本轮是否产生了新的 _need_svt_4000 或 _need_svt_p6 文件
func runOnce(rootDir string) (hasRetry bool) {
	// ── Step 1：扫描 ──────────────────────────────────────────
	logf("[SCAN] 开始扫描视频文件...")
	direct, toProbe, err := scanVideos(rootDir)
	if err != nil {
		log.Fatalf("[SCAN] 扫描失败: %v", err)
	}
	logf("[SCAN] 共找到 %d 个文件（直接入队 %d，待 probe %d）",
		len(direct)+len(toProbe), len(direct), len(toProbe))

	if len(direct)+len(toProbe) == 0 {
		logf("[MAIN] 没有需要处理的文件。")
		return false
	}

	// ── Step 2：并发 Probe + 分类 ─────────────────────────────
	tasks, probeFailures := probeAll(toProbe, direct)

	var copyTasks, nvencTasks, svtTasks, svt4000Tasks, svtP6Tasks []Task
	for _, t := range tasks {
		switch t.Kind {
		case TaskCopy:
			copyTasks = append(copyTasks, t)
		case TaskNVEnc:
			nvencTasks = append(nvencTasks, t)
		case TaskSVTAV1:
			svtTasks = append(svtTasks, t)
		case TaskNeedSVT4000:
			svt4000Tasks = append(svt4000Tasks, t)
		case TaskNeedSVTP6:
			svtP6Tasks = append(svtP6Tasks, t)
		}
	}

	logf("\n[PLAN] 任务清单:")
	logf("       COPY              : %d 个（码率 < %dkbps）", len(copyTasks), SKIP_BITRATE_KBP)
	logf("       NVENC             : %d 个（码率 > %dkbps）", len(nvencTasks), HIGH_BITRATE_KBP)
	logf("       SVT-AV1 3000k     : %d 个（码率 %d~%dkbps）", len(svtTasks), SKIP_BITRATE_KBP, HIGH_BITRATE_KBP)
	logf("       SVT-AV1 retry4000 : %d 个（_need_svt_4000）", len(svt4000Tasks))
	logf("       SVT-AV1 retryP6   : %d 个（_need_svt_p6）", len(svtP6Tasks))
	logf("       Probe 失败        : %d 个", len(probeFailures))

	if dryRun {
		printDryrunPlan(copyTasks, nvencTasks, svtTasks, svt4000Tasks, svtP6Tasks, probeFailures)
		return false
	}

	results := make([]Result, 0)
	results = append(results, probeFailures...)

	// ── Step 3a：并发 Copy ────────────────────────────────────
	if len(copyTasks) > 0 {
		workers := ioWorkerCount()
		logf("[STAGE] 并发 copy %d 个低码率文件（%d workers）...", len(copyTasks), workers)
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
		go func() { wg.Wait(); close(resCh) }()
		for r := range resCh {
			results = append(results, r)
		}
		logf("[STAGE] Copy 阶段完成。")
	}

	// ── Step 3b：串行 NVEncC AV1 ─────────────────────────────
	if len(nvencTasks) > 0 {
		logf("[STAGE] NVEncC AV1 编码 %d 个文件（串行）...", len(nvencTasks))
		for i, task := range nvencTasks {
			results = append(results, execEncode(task, i+1, len(nvencTasks)))
		}
		logf("[STAGE] NVEncC 阶段完成。")
	}

	// ── Step 3c：串行 SVT-AV1（标准 + 两种重试）─────────────
	svtAll := append(svtTasks, append(svt4000Tasks, svtP6Tasks...)...)
	if len(svtAll) > 0 {
		logf("[STAGE] SVT-AV1 编码 %d 个文件（串行）...", len(svtAll))
		for i, task := range svtAll {
			results = append(results, execEncode(task, i+1, len(svtAll)))
		}
		logf("[STAGE] SVT-AV1 阶段完成。")
	}

	// ── 汇总 ─────────────────────────────────────────────────
	var cntTranscoded, cntCopiedLow, cntCopiedBig, cntSVT4000, cntSVTP6, cntItDoesntWork, cntFailed int
	for _, r := range results {
		switch r.Status {
		case StatusTranscoded:
			cntTranscoded++
		case StatusCopiedLowBR:
			cntCopiedLow++
		case StatusCopiedTooBig:
			cntCopiedBig++
		case StatusNeedSVT4000:
			cntSVT4000++
			hasRetry = true
		case StatusNeedSVTP6:
			cntSVTP6++
			hasRetry = true
		case StatusItDoesntWork:
			cntItDoesntWork++
		case StatusFailed:
			cntFailed++
		}
	}

	logf("\n╔══════════════ 本轮处理完成 ══════════════╗")
	logf("║ 成功转码                  : %d", cntTranscoded)
	logf("║ copy 原文件 (码率过低)    : %d", cntCopiedLow)
	logf("║ copy 原文件 (体积未减小)  : %d", cntCopiedBig)
	logf("║ 标记为 need_svt_4000      : %d", cntSVT4000)
	logf("║ 标记为 need_svt_p6        : %d", cntSVTP6)
	logf("║ 标记为 it_doesnt_work     : %d", cntItDoesntWork)
	logf("║ 失败                      : %d", cntFailed)
	logf("╚══════════════════════════════════════════╝")

	return hasRetry
}

func printDryrunPlan(copyTasks, nvencTasks, svtTasks, svt4000Tasks, svtP6Tasks []Task, probeFailures []Result) {
	logf("\n[DRYRUN] ══════ COPY（%d 个）══════", len(copyTasks))
	for i, t := range copyTasks {
		fi, _ := os.Stat(t.Path)
		var sizeMB float64
		if fi != nil {
			sizeMB = float64(fi.Size()) / 1024 / 1024
		}
		dir := filepath.Dir(t.Path)
		stem := strings.TrimSuffix(filepath.Base(t.Path), filepath.Ext(t.Path))
		dst := filepath.Join(dir, stem+"_copy_original.mp4")
		bitrateTag := ""
		if t.Info.BitrateEstimated {
			bitrateTag = "（估算）"
		}
		logf("[DRYRUN] COPY  [%d/%d] %s", i+1, len(copyTasks), t.Path)
		logf("               码率: %d kbps%s  大小: %.2fMB  输出: %s", t.Info.Bitrate, bitrateTag, sizeMB, dst)
	}

	logf("\n[DRYRUN] ══════ NVEncC AV1（%d 个）══════", len(nvencTasks))
	for i, t := range nvencTasks {
		fi, _ := os.Stat(t.Path)
		var sizeMB float64
		if fi != nil {
			sizeMB = float64(fi.Size()) / 1024 / 1024
		}
		dir := filepath.Dir(t.Path)
		stem := strings.TrimSuffix(filepath.Base(t.Path), filepath.Ext(t.Path))
		dst := outputPath(dir, stem, "av1", NVENC_TARGET_KBP)
		bitrateTag := ""
		if t.Info.BitrateEstimated {
			bitrateTag = "（估算）"
		}
		logf("[DRYRUN] NVENC [%d/%d] %s", i+1, len(nvencTasks), t.Path)
		logf("               码率: %d kbps%s → %d kbps  大小: %.2fMB  输出: %s",
			t.Info.Bitrate, bitrateTag, NVENC_TARGET_KBP, sizeMB, dst)
	}

	logf("\n[DRYRUN] ══════ SVT-AV1 3000k（%d 个）══════", len(svtTasks))
	for i, t := range svtTasks {
		fi, _ := os.Stat(t.Path)
		var sizeMB float64
		if fi != nil {
			sizeMB = float64(fi.Size()) / 1024 / 1024
		}
		dir := filepath.Dir(t.Path)
		stem := strings.TrimSuffix(filepath.Base(t.Path), filepath.Ext(t.Path))
		dst := outputPath(dir, stem, "svt-av1", SVT_TARGET_KBP)
		bitrateTag := ""
		if t.Info.BitrateEstimated {
			bitrateTag = "（估算）"
		}
		logf("[DRYRUN] SVT   [%d/%d] %s", i+1, len(svtTasks), t.Path)
		logf("               码率: %d kbps%s → %d kbps  大小: %.2fMB  输出: %s",
			t.Info.Bitrate, bitrateTag, SVT_TARGET_KBP, sizeMB, dst)
	}

	logf("\n[DRYRUN] ══════ SVT-AV1 retry4000（%d 个）══════", len(svt4000Tasks))
	for i, t := range svt4000Tasks {
		logf("[DRYRUN] SVT4K [%d/%d] %s → %d kbps", i+1, len(svt4000Tasks), t.Path, SVT_RETRY_KBP)
	}

	logf("\n[DRYRUN] ══════ SVT-AV1 retryP6（%d 个）══════", len(svtP6Tasks))
	for i, t := range svtP6Tasks {
		logf("[DRYRUN] SVTP6 [%d/%d] %s → %d kbps preset6", i+1, len(svtP6Tasks), t.Path, SVT_TARGET_KBP)
	}

	if len(probeFailures) > 0 {
		logf("\n[DRYRUN] ══════ Probe 失败（%d 个，将跳过）══════", len(probeFailures))
		for _, r := range probeFailures {
			logf("[DRYRUN] SKIP  %s — %s", r.Path, r.Note)
		}
	}
	logf("\n[DRYRUN] 以上为完整执行计划，未执行任何实际操作。")
}

// ================== --checklog 报告生成 ==================

// encodeEvent 代表日志中一次编码任务的完整记录
type encodeEvent struct {
	path       string
	origSizeMB float64
	origBR     int
	encoder    string // nvenc / svt-av1 / svt-av1(retry4000) / svt-av1(retryp6)
	status     string // transcoded / failed / copied_too_big / need_svt_4000 / need_svt_p6 / it_doesnt_work
	logLines   []string
}

// runCheckLog 解析 logPath，从最近一次 [SCAN] 行开始，生成报告文件到 reportDir
func runCheckLog(logPath, reportDir string) {
	f, err := os.Open(logPath)
	if err != nil {
		logf("[CHECKLOG] 无法打开日志文件: %v", err)
		return
	}
	defer f.Close()

	// 读取全部行
	var allLines []string
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		allLines = append(allLines, scanner.Text())
	}

	// 从末尾找最近一次 [SCAN] 开始扫描视频文件...
	startIdx := -1
	for i := len(allLines) - 1; i >= 0; i-- {
		if strings.Contains(allLines[i], "[SCAN] 开始扫描视频文件") {
			startIdx = i
			break
		}
	}
	if startIdx < 0 {
		logf("[CHECKLOG] 日志中未找到 [SCAN] 开始扫描视频文件... 行")
		return
	}
	lines := allLines[startIdx:]

	// 判断是新格式（有 [ENCODE] 标记）还是旧格式
	isNewFormat := false
	for _, l := range lines {
		if strings.Contains(l, "[ENCODE] ") {
			isNewFormat = true
			break
		}
	}

	var events []encodeEvent
	var current *encodeEvent

	if isNewFormat {
		// 新格式：解析 [ENCODE] 行和 [RESULT] 行
		for _, line := range lines {
			if idx := strings.Index(line, "[ENCODE] "); idx >= 0 {
				if current != nil {
					events = append(events, *current)
				}
				current = &encodeEvent{}
				kv := line[idx+len("[ENCODE] "):]
				if pi := strings.Index(kv, " path="); pi >= 0 {
					rest := kv[:pi]
					current.path = strings.TrimSpace(kv[pi+len(" path="):])
					kv = rest
				}
				for _, part := range strings.Fields(kv) {
					k, v, ok := strings.Cut(part, "=")
					if !ok {
						continue
					}
					switch k {
					case "encoder":
						current.encoder = v
					case "origSizeMB":
						current.origSizeMB, _ = strconv.ParseFloat(v, 64)
					case "origBR":
						current.origBR, _ = strconv.Atoi(v)
					}
				}
				current.logLines = append(current.logLines, line)
				continue
			}
			if current != nil && strings.Contains(line, "[RESULT] status=") {
				idx := strings.Index(line, "[RESULT] status=")
				current.status = strings.TrimSpace(line[idx+len("[RESULT] status="):])
				current.logLines = append(current.logLines, line)
				continue
			}
			if current != nil {
				current.logLines = append(current.logLines, line)
			}
		}
	} else {
		// 旧格式：解析 [N/M] NVENC / [N/M] SVT-AV1 任务头行，从命令行提取完整路径
		reHeader := regexp.MustCompile(`\[\d+/\d+\] (NVENC|SVT-AV1)  `)
		reBRSize := regexp.MustCompile(`\((\d+) kbps [^,]+, ([\d.]+)MB\)`)
		for _, line := range lines {
			if reHeader.MatchString(line) {
				if current != nil {
					events = append(events, *current)
				}
				current = &encodeEvent{}
				if strings.Contains(line, "] NVENC  ") {
					current.encoder = "nvenc"
				} else {
					current.encoder = "svt"
				}
				if m := reBRSize.FindStringSubmatch(line); len(m) == 3 {
					current.origBR, _ = strconv.Atoi(m[1])
					current.origSizeMB, _ = strconv.ParseFloat(m[2], 64)
				}
				current.logLines = append(current.logLines, line)
				continue
			}
			if current == nil {
				continue
			}
			current.logLines = append(current.logLines, line)
			// NVEnc 命令行 → 提取完整输入路径（在 -i 与 -o 之间）
			if current.path == "" && strings.Contains(line, "  [NVEnc] ") {
				if i := strings.Index(line, " -i "); i >= 0 {
					rest := line[i+4:]
					if j := strings.Index(rest, " -o "); j >= 0 {
						current.path = rest[:j]
					}
				}
			}
			// SVT-AV1 Pass1 命令行 → 提取完整输入路径（在 -i 与 -vf 之间）
			if current.path == "" && strings.Contains(line, "  [SVT-AV1] Pass1:") {
				if i := strings.Index(line, " -i "); i >= 0 {
					rest := line[i+4:]
					if j := strings.Index(rest, " -vf "); j >= 0 {
						current.path = rest[:j]
					}
				}
			}
			// 结果判断
			if strings.Contains(line, "✗ 编码失败:") {
				current.status = "failed"
			} else if strings.Contains(line, "✓ [copy原文件(体积未减小)]") {
				current.status = "copied_too_big"
			} else if strings.Contains(line, "✓ [转码成功]") {
				current.status = "transcoded"
			}
		}
	}
	if current != nil {
		events = append(events, *current)
	}

	// 分类
	var failedEvents, tooBigEvents []encodeEvent
	for _, e := range events {
		switch e.status {
		case "failed":
			failedEvents = append(failedEvents, e)
		case "copied_too_big", "it_doesnt_work":
			tooBigEvents = append(tooBigEvents, e)
		}
	}

	// 检查残留的 need_svt 标记
	var pendingRetry []encodeEvent
	for _, e := range events {
		if e.status == "need_svt_4000" || e.status == "need_svt_p6" {
			pendingRetry = append(pendingRetry, e)
		}
	}

	// 生成报告文件
	ts := time.Now().Format("20060102_150405")
	reportPath := filepath.Join(reportDir, "transcode_report_"+ts+".log")
	rf, err := os.Create(reportPath)
	if err != nil {
		logf("[CHECKLOG] 无法创建报告文件: %v", err)
		return
	}
	defer rf.Close()

	w := bufio.NewWriter(rf)
	writeLine := func(s string) { fmt.Fprintln(w, s) }

	writeLine("================================================================")
	writeLine(fmt.Sprintf("transcode 报告  生成时间：%s", time.Now().Format("2006-01-02 15:04:05")))
	writeLine(fmt.Sprintf("日志来源：%s（从第 %d 行起）", logPath, startIdx+1))
	writeLine("================================================================")

	// 第一部分：失败项
	writeLine(fmt.Sprintf("\n━━━━━━━━━━━━ 编码失败（%d 项）━━━━━━━━━━━━", len(failedEvents)))
	for _, e := range failedEvents {
		writeLine("")
		for _, l := range e.logLines {
			writeLine(l)
		}
	}

	// 第二部分：体积不达标项
	writeLine(fmt.Sprintf("\n━━━━━━━━━━━━ 体积不达标（%d 项）━━━━━━━━━━━━", len(tooBigEvents)))
	for _, e := range tooBigEvents {
		writeLine("")
		for _, l := range e.logLines {
			writeLine(l)
		}
	}

	// 残留重试标记（异常情况）
	if len(pendingRetry) > 0 {
		writeLine(fmt.Sprintf("\n━━━━━━━━━━━━ 未处理的重试标记（%d 项，需下次运行）━━━━━━━━━━━━", len(pendingRetry)))
		for _, e := range pendingRetry {
			writeLine(fmt.Sprintf("  [%s] %s", e.status, e.path))
		}
	}

	// 附录：体积不达标文件清单，按原始码率从大到小
	sort.Slice(tooBigEvents, func(i, j int) bool {
		return tooBigEvents[i].origBR > tooBigEvents[j].origBR
	})
	writeLine("\n================================================================")
	writeLine("附录：体积不达标文件清单（按原始码率从大到小）")
	writeLine("格式：文件路径, 体积(MB), 原始码率(kbps), 编码器")
	writeLine("================================================================")
	for _, e := range tooBigEvents {
		writeLine(fmt.Sprintf("%s, %.2f, %d, %s", e.path, e.origSizeMB, e.origBR, e.encoder))
	}

	w.Flush()
	logf("[CHECKLOG] 报告已生成：%s（失败 %d 项，体积不达标 %d 项）",
		reportPath, len(failedEvents), len(tooBigEvents))
}