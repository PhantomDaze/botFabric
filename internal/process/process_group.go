package process

import (
	"errors"
	"log/slog"
	"os/exec"
	"sync"
	"time"
)

// sigForceKill 是强制终止进程组时使用的信号；由平台文件定义。
var sigForceKill = killSignal()

// prepareProcessGroup 为外部插件进程设置独立进程组。
//
// 外部插件可能 fork 出子孙进程；Runtime 只持有直接子进程的句柄，
// 无法对孙进程单独发信号。把插件进程放进独立进程组后，终止时
// 向进程组发信号即可回收整棵进程树，避免孤儿进程残留。
func prepareProcessGroup(command *exec.Cmd) {
	configureProcessGroup(command)
}

// terminateProcessGroup 向进程组发送信号，返回是否支持进程组语义。
//
// pgid 为 0 表示进程组不可用，调用方应回退为只终止直接子进程。
func terminateProcessGroup(pgid int, signal osSignal) error {
	if pgid == 0 {
		return errors.New("外部插件进程组未建立")
	}
	return signalProcessGroup(pgid, signal)
}

// applyMemoryLimit 为外部插件进程设置内存上限。
//
// 实现为 RSS 轮询监控：bytes 为 0 表示清单未声明，跳过监控。
// 通过 monitor 返回的停止函数结束监控 goroutine。
func applyMemoryLimit(command *exec.Cmd, bytes int64, logger *slog.Logger, pluginID string, onExceeded func()) (stop func()) {
	if bytes <= 0 || command == nil || command.Process == nil {
		return func() {}
	}
	pid := command.Process.Pid
	done := make(chan struct{})
	once := sync.OnceFunc(func() { close(done) })
	go func() {
		// 连续超限次数：瞬时峰值不立即杀，持续超限才终止。
		overLimit := 0
		ticker := time.NewTicker(memorySampleInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
			}
			rss, err := readProcessRSS(pid)
			if err != nil {
				// 进程退出后 /proc 读取失败，监控自然结束。
				return
			}
			if rss > bytes {
				overLimit++
				if overLimit >= memoryOverLimitSamples {
					logger.Error("外部插件内存超限，强制终止", "plugin_id", pluginID, "rss_bytes", rss, "limit_bytes", bytes)
					if onExceeded != nil {
						onExceeded()
					}
					return
				}
			} else {
				overLimit = 0
			}
		}
	}()
	return once
}

const (
	// memorySampleInterval 是 RSS 采样周期。
	memorySampleInterval = 200 * time.Millisecond
	// memoryOverLimitSamples 是连续超限采样次数阈值。
	memoryOverLimitSamples = 5
)
