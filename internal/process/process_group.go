package process

import (
	"errors"
	"os/exec"
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
