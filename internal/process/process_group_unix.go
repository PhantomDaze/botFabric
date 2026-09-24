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
