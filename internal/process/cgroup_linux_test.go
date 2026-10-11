//go:build linux

package process

import (
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestStartTransientServiceRequestsIsolationWithoutMemoryLimit(t *testing.T) {
	tmp := t.TempDir()
	binDir := filepath.Join(tmp, "bin")
	if err := os.Mkdir(binDir, 0o700); err != nil {
		t.Fatal(err)
	}
	argsPath := filepath.Join(tmp, "args")
	systemdRun := filepath.Join(binDir, "systemd-run")
	systemctl := filepath.Join(binDir, "systemctl")
	writeExecutable(t, systemdRun, "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$BOTFABRIC_TEST_ARGS\"\n")
	writeExecutable(t, systemctl, "#!/bin/sh\nprintf '1\\n'\n")
	t.Setenv("PATH", binDir)
	t.Setenv("BOTFABRIC_TEST_ARGS", argsPath)
	t.Setenv("TMPDIR", tmp)

	command := exec.Command("/bin/echo", "plugin-arg")
	unit, pid, _, _, logDir, err := startTransientService(systemdRun, command, 0, true, true)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(logDir)
	if unit == "" || pid != 1 {
		t.Fatalf("unit/pid = %q/%d", unit, pid)
	}
	content, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Split(strings.TrimSpace(string(content)), "\n")
	if !containsAdjacent(args, "-p", "PrivateNetwork=yes") || !containsAdjacent(args, "-p", "NoNewPrivileges=yes") {
		t.Fatalf("systemd-run 参数未启用隔离: %q", args)
	}
	if containsAdjacent(args, "-p", "MemoryMax=0") || containsAdjacent(args, "-p", "MemoryMax=1") {
		t.Fatalf("未配置 memory 时不应生成 MemoryMax 属性: %q", args)
	}
	if !containsAdjacent(args, "--", "/bin/echo") || !containsAdjacent(args, "/bin/echo", "plugin-arg") {
		t.Fatalf("插件入口参数缺失: %q", args)
	}
}

func TestStartTransientServiceNoNewPrivilegesWithoutMemoryLimit(t *testing.T) {
	tmp := t.TempDir()
	binDir := filepath.Join(tmp, "bin")
	if err := os.Mkdir(binDir, 0o700); err != nil {
		t.Fatal(err)
	}
	argsPath := filepath.Join(tmp, "args")
	systemdRun := filepath.Join(binDir, "systemd-run")
	systemctl := filepath.Join(binDir, "systemctl")
	writeExecutable(t, systemdRun, "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$BOTFABRIC_TEST_ARGS\"\n")
	writeExecutable(t, systemctl, "#!/bin/sh\nprintf '1\\n'\n")
	t.Setenv("PATH", binDir)
	t.Setenv("BOTFABRIC_TEST_ARGS", argsPath)
	t.Setenv("TMPDIR", tmp)

	_, _, _, _, logDir, err := startTransientService(systemdRun, exec.Command("/bin/true"), 0, false, true)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(logDir)
	content, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Split(strings.TrimSpace(string(content)), "\n")
	if !containsAdjacent(args, "-p", "NoNewPrivileges=yes") {
		t.Fatalf("systemd-run 参数未启用 NoNewPrivileges: %q", args)
	}
	if containsAdjacent(args, "-p", "PrivateNetwork=yes") || containsAdjacent(args, "-p", "MemoryMax=0") {
		t.Fatalf("NNP-only 不应隐式启用其他限制: %q", args)
	}
}

func TestStartCgroupIsolationFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name           string
		privateNetwork bool
		noNewPrivs     bool
	}{
		{name: "private-network", privateNetwork: true},
		{name: "no-new-privileges", noNewPrivs: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			tmp := t.TempDir()
			binDir := filepath.Join(tmp, "bin")
			if err := os.Mkdir(binDir, 0o700); err != nil {
				t.Fatal(err)
			}
			writeExecutable(t, filepath.Join(binDir, "systemd-run"), "#!/bin/sh\nexit 42\n")
			t.Setenv("PATH", binDir)
			t.Setenv("TMPDIR", tmp)

			enforcer, err := startCgroupMemoryLimit(exec.Command("/bin/true"), 0, test.privateNetwork, test.noNewPrivs, slog.New(slog.NewTextHandler(os.Stderr, nil)), "test.plugin")
			if err == nil || enforcer != nil {
				t.Fatalf("隔离启动失败应返回错误而不降级: enforcer=%#v err=%v", enforcer, err)
			}
			entries, err := os.ReadDir(tmp)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if strings.HasPrefix(entry.Name(), "botfabric-log-") {
					t.Fatalf("失败启动遗留临时目录: %s", entry.Name())
				}
			}
		})
	}
}

func TestStartCgroupMemoryLimitKeepsMemoryFallback(t *testing.T) {
	tmp := t.TempDir()
	binDir := filepath.Join(tmp, "bin")
	if err := os.Mkdir(binDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeExecutable(t, filepath.Join(binDir, "systemd-run"), "#!/bin/sh\nexit 42\n")
	t.Setenv("PATH", binDir)
	t.Setenv("TMPDIR", tmp)

	enforcer, err := startCgroupMemoryLimit(exec.Command("/bin/true"), 64*1024*1024, false, false, slog.New(slog.NewTextHandler(os.Stderr, nil)), "test.plugin")
	if err != nil || enforcer != nil {
		t.Fatalf("memory-only 失败应允许调用方回退 RSS: enforcer=%#v err=%v", enforcer, err)
	}
}

func writeExecutable(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatal(err)
	}
}

func containsAdjacent(values []string, first, second string) bool {
	for i := 0; i+1 < len(values); i++ {
		if values[i] == first && values[i+1] == second {
			return true
		}
	}
	return false
}
