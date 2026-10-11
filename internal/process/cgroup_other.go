//go:build !linux

package process

import (
	"errors"
	"log/slog"
	"os/exec"
)

// startCgroupMemoryLimit 在非 Linux 平台不可用；显式请求隔离属性时拒绝启动。
func startCgroupMemoryLimit(_ *exec.Cmd, _ int64, privateNetwork, noNewPrivileges bool, _ *slog.Logger, _ string) (*memoryEnforcer, error) {
	if privateNetwork || noNewPrivileges {
		return nil, errors.New("Linux systemd 进程隔离属性不可用")
	}
	return nil, nil
}

// waitManagedProcess 在非 Linux 平台不会被调用（托管路径只在 Linux 存在）。
func waitManagedProcess(command *exec.Cmd) error {
	return command.Wait()
}
