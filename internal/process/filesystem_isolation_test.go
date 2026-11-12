//go:build linux

package process

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidateProtectHomeSocketPath(t *testing.T) {
	for _, path := range []string{
		"/home/antom/botfabric/plugin.sock",
		"/root/botfabric/plugin.sock",
		"/run/user/1000/botfabric/plugin.sock",
	} {
		if err := validateProtectHomeSocketPath(path); err == nil {
			t.Fatalf("ProtectHome 应拒绝位于受限目录中的 Socket: %s", path)
		}
	}
	for _, path := range []string{
		"/opt/DEV/bot/run/plugin.sock",
		"/var/lib/botfabric/plugin.sock",
		"/tmp/botfabric/plugin.sock",
		"/home-other/botfabric/plugin.sock",
	} {
		if err := validateProtectHomeSocketPath(path); err != nil {
			t.Fatalf("ProtectHome 不应拒绝受限目录之外的 Socket %s: %v", path, err)
		}
	}
}

func TestValidateProtectHomeSocketPathResolvesSymlinks(t *testing.T) {
	link := filepath.Join(t.TempDir(), "home-link")
	if err := os.Symlink("/home", link); err != nil {
		t.Skipf("创建符号链接失败: %v", err)
	}
	if err := validateProtectHomeSocketPath(filepath.Join(link, "plugin.sock")); err == nil {
		t.Fatal("ProtectHome 应拒绝经符号链接指向 /home 的 Socket")
	}
}

func TestStartProtectHomeRejectsSocketUnderRestrictedPath(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	transport, err := NewGRPCTransport(GRPCTransportConfig{Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	manifest := processManifest()
	manifest.Entry = "/bin/true"
	manifest.Limits.ProtectHome = true
	session := Session{PluginID: manifest.ID, Token: "runtime-token", ExpiresAt: time.Now().Add(time.Minute)}
	err = transport.StartWithSession(context.Background(), manifest, "/home/botfabric-test/plugin.sock", session)
	if err == nil || !strings.Contains(err.Error(), "ProtectHome") {
		t.Fatalf("ProtectHome 应拒绝位于 /home 的 Socket，实际错误: %v", err)
	}
}
