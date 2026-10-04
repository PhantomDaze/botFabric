//go:build !linux

package process

import (
	"log/slog"
	"os/exec"
)

// startCgroupMemoryLimit 在非 Linux 平台不可用，返回 nil 让调用方回退 RSS 监控。
func startCgroupMemoryLimit(*exec.Cmd, int64, *slog.Logger, string) *memoryEnforcer {
	return nil
}

// waitManagedProcess 在非 Linux 平台不会被调用（托管路径只在 Linux 存在）。
func waitManagedProcess(command *exec.Cmd) error {
	return command.Wait()
}
