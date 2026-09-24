//go:build unix

package process

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

// osSignal 是进程组信号的平台无关抽象。
type osSignal = syscall.Signal

// killSignal 返回 Unix 平台强制终止进程组使用的信号。
func killSignal() osSignal { return syscall.SIGKILL }

// configureProcessGroup 在 Unix 平台为外部插件进程创建独立进程组。
func configureProcessGroup(command *exec.Cmd) {
	if command.SysProcAttr == nil {
		command.SysProcAttr = &syscall.SysProcAttr{}
	}
	command.SysProcAttr.Setpgid = true
}

// signalProcessGroup 向进程组整体发送信号。
func signalProcessGroup(pgid int, signal osSignal) error {
	// ESRCH 表示进程组已不存在，视为已回收。
	if err := syscall.Kill(-pgid, signal); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}

// processGroupID 返回进程所属进程组 ID；失败返回 0。
func processGroupID(process *os.Process) int {
	// Wait4 会修改进程状态，改用 /proc 读取 pgid，避免干扰 exec.Cmd.Wait。
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", process.Pid))
	if err != nil {
		return 0
	}
	// stat 第 4 个字段是 pgrp；comm 字段可能含空格，因此先定位最后一个 ) 之后。
	text := string(data)
	if idx := strings.LastIndex(text, ")"); idx >= 0 && idx+2 < len(text) {
		fields := strings.Fields(text[idx+2:])
		// fields[0]=state, [1]=ppid, [2]=pgrp
		if len(fields) >= 3 {
			if pgid, err := strconv.Atoi(fields[2]); err == nil {
				return pgid
			}
		}
	}
	return 0
}

// setMemoryLimit 在 Unix 平台读取进程当前 RSS 字节数。
//
// 注意不使用 RLIMIT_AS：Go/JVM 等运行时保留远超物理内存的虚拟
// 地址空间，RLIMIT_AS 会直接压死进程初始化。改为由 transport 轮询
// RSS 并在超限时终止进程。
func readProcessRSS(pid int) (int64, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			break
		}
		kib, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("解析 VmRSS: %w", err)
		}
		return kib * 1024, nil
	}
	return 0, errors.New("未找到 VmRSS 字段")
}
