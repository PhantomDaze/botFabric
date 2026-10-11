package process

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidatePrivateTmpSocketPath(t *testing.T) {
	for _, root := range []string{"/tmp", "/var/tmp"} {
		subdirectory := filepath.Join(root, "botfabric-private-tmp-test-"+filepath.Base(t.TempDir()))
		for _, relative := range []string{"plugin.sock", "runtime/plugin.sock"} {
			path := filepath.Join(subdirectory, relative)
			t.Run(path, func(t *testing.T) {
				if err := validatePrivateTmpSocketPath(path); err == nil {
					t.Fatalf("PrivateTmp 应拒绝位于临时目录中的 Socket: %s", path)
				}
			})
		}
	}

	for _, path := range []string{
		"/run/botfabric/plugin.sock",
		"/var/run/botfabric/plugin.sock",
		"/var/tmp-other/plugin.sock",
	} {
		t.Run(path, func(t *testing.T) {
			if err := validatePrivateTmpSocketPath(path); err != nil {
				t.Fatalf("PrivateTmp 不应拒绝临时目录之外的 Socket %s: %v", path, err)
			}
		})
	}
}

func TestValidatePrivateTmpSocketPathResolvesSymlinks(t *testing.T) {
	root := t.TempDir()
	link := filepath.Join(root, "tmp-link")
	if err := os.Symlink("/tmp", link); err != nil {
		t.Skipf("创建符号链接失败: %v", err)
	}
	if err := validatePrivateTmpSocketPath(filepath.Join(link, "plugin.sock")); err == nil {
		t.Fatal("PrivateTmp 应拒绝经符号链接指向 /tmp 的 Socket")
	}
}
