// rename_report.go — 独立工具，读取 --checklog 生成的报告附录，批量重命名体积不达标文件
//
// 用法：
//   rename_report.exe <报告文件路径>
//
// 重命名规则：
//   编码器 nvenc        → _av1_6000k 替换为 _need_svt_4000
//   编码器 svt-av1 且 码率 > 4000kbps → _svt-av1_3000k 替换为 _need_svt_p6
//   其余情况不操作

package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "用法: rename_report.exe <报告文件路径>")
		os.Exit(1)
	}
	reportPath := os.Args[1]

	f, err := os.Open(reportPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "无法打开报告文件: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	// 定位"附录："部分
	var appendixLines []string
	inAppendix := false
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "附录：体积不达标文件清单") {
			inAppendix = true
			continue
		}
		if inAppendix {
			// 跳过分隔线和格式说明行
			if strings.HasPrefix(line, "=") || strings.HasPrefix(line, "格式：") {
				continue
			}
			if strings.TrimSpace(line) == "" {
				continue
			}
			appendixLines = append(appendixLines, line)
		}
	}

	if len(appendixLines) == 0 {
		fmt.Println("附录中没有需要处理的文件。")
		return
	}

	var cntRenamed, cntSkipped, cntNotFound int

	for _, line := range appendixLines {
		// 格式：文件路径, 体积(MB), 原始码率(kbps), 编码器
		// path 在最前，可能含逗号，所以从末尾解析
		parts := strings.Split(line, ", ")
		if len(parts) < 4 {
			fmt.Printf("  [跳过] 格式不合法: %s\n", line)
			cntSkipped++
			continue
		}
		// 取最后三个字段，其余拼回作为路径
		encoder := strings.TrimSpace(parts[len(parts)-1])
		brStr := strings.TrimSpace(parts[len(parts)-2])
		// sizeMB := parts[len(parts)-3]  // 暂不使用
		filePath := strings.Join(parts[:len(parts)-3], ", ")

		br, _ := strconv.Atoi(brStr)

		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			fmt.Printf("  [不存在] %s\n", filePath)
			cntNotFound++
			continue
		}

		dir := filepath.Dir(filePath)
		base := filepath.Base(filePath)
		ext := filepath.Ext(base)
		stem := strings.TrimSuffix(base, ext)

		var newStem string
		switch {
		case encoder == "nvenc":
			if strings.HasSuffix(stem, "_av1_6000k") {
				newStem = stem[:len(stem)-len("_av1_6000k")] + "_need_svt_4000"
			} else {
				fmt.Printf("  [跳过] nvenc 文件名不含 _av1_6000k: %s\n", base)
				cntSkipped++
				continue
			}
		case encoder == "svt-av1" && br > 4000:
			if strings.HasSuffix(stem, "_svt-av1_3000k") {
				newStem = stem[:len(stem)-len("_svt-av1_3000k")] + "_need_svt_p6"
			} else {
				fmt.Printf("  [跳过] svt 文件名不含 _svt-av1_3000k: %s\n", base)
				cntSkipped++
				continue
			}
		default:
			// svt 且码率 ≤ 4000，或 svt4000/svtp6 等，不操作
			fmt.Printf("  [不操作] %s（encoder=%s, br=%d kbps）\n", base, encoder, br)
			cntSkipped++
			continue
		}

		newPath := filepath.Join(dir, newStem+ext)
		if err := os.Rename(filePath, newPath); err != nil {
			fmt.Printf("  [失败] 重命名失败: %v\n", err)
			cntSkipped++
			continue
		}
		fmt.Printf("  [重命名] %s\n         → %s\n", base, newStem+ext)
		cntRenamed++
	}

	fmt.Printf("\n完成：重命名 %d 个，跳过 %d 个，文件不存在 %d 个。\n", cntRenamed, cntSkipped, cntNotFound)
}
