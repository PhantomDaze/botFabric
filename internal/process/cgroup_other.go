//go:build !linux

package process

import (
	"errors"
	"log/slog"
	"os/exec"
)

// startCgroupMemoryLimit 在非 Linux 平台不可用；显式请求网络隔离时返回错误，拒绝无隔离启动。
func startCgroupMemoryLimit(_ *exec.Cmd, _ int64, privateNetwork bool, _ *slog.Logger, _ string) (*memoryEnforcer, error) {
	if privateNetwork {
		return nil, errors.New("Linux systemd 网络命名空间隔离不可用")
	}
	return nil, nil
}

// waitManagedProcess 在非 Linux 平台不会被调用（托管路径只在 Linux 存在）。
func waitManagedProcess(command *exec.Cmd) error {
	return command.Wait()
}
