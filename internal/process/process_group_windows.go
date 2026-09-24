//go:build windows

package process

import (
	"errors"
	"os"
	"os/exec"
)

// osSignal 是进程组信号的平台无关抽象；Windows 用 int 占位。
type osSignal = int

// killSignal 在 Windows 平台返回占位信号；进程组终止本身不可用。
func killSignal() osSignal { return 0 }

// configureProcessGroup 在 Windows 平台为空实现。
// Windows 没有 POSIX 进程组语义，Runtime 只能终止直接子进程。
func configureProcessGroup(*exec.Cmd) {}

// signalProcessGroup 在 Windows 平台不可用。
func signalProcessGroup(int, osSignal) error {
	return errors.New("Windows 平台不支持进程组终止")
}

// processGroupID 在 Windows 平台恒返回 0。
func processGroupID(*os.Process) int { return 0 }

// readProcessRSS 在 Windows 平台不可用；返回错误使监控 goroutine 退出。
func readProcessRSS(int) (int64, error) {
	return 0, errors.New("Windows 平台不支持 RSS 读取")
}
