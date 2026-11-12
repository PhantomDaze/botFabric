//go:build linux

package process

import (
	"fmt"
	"net"
	"syscall"
)

// Accept 接受 Unix Socket 连接，并只交付由本 transport 启动的插件进程。
// 同 UID 的其他进程即使持有会话令牌也无法建立 Session。
func (l *peerCredListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if err := verifyPeerPID(conn, l.expectedPID); err != nil {
			l.rejected.Add(1)
			_ = conn.Close()
			continue
		}
		return conn, nil
	}
}

// verifyPeerPID 通过 SO_PEERCRED 校验连接方进程 PID。
func verifyPeerPID(conn net.Conn, expectedPID int) error {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return fmt.Errorf("外部插件连接不是 Unix Socket")
	}
	raw, err := unixConn.SyscallConn()
	if err != nil {
		return fmt.Errorf("读取外部插件连接: %w", err)
	}
	var cred *syscall.Ucred
	var credErr error
	if err := raw.Control(func(fd uintptr) {
		cred, credErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return fmt.Errorf("读取外部插件对端凭据: %w", err)
	}
	if credErr != nil {
		return fmt.Errorf("读取外部插件对端凭据: %w", credErr)
	}
	if expectedPID > 0 && int(cred.Pid) != expectedPID {
		return fmt.Errorf("外部插件连接 PID %d 与启动进程 PID %d 不一致", cred.Pid, expectedPID)
	}
	return nil
}
