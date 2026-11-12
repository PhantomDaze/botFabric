//go:build !linux

package process

import "net"

// Accept 在非 Linux 平台暂不对端凭据校验。
// SO_PEERCRED 仅 Linux 提供；生产部署目标为 Linux。
func (l *peerCredListener) Accept() (net.Conn, error) {
	return l.Listener.Accept()
}
