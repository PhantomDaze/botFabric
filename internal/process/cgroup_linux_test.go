//go:build linux

package process

import (
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
	unit, pid, _, _, logDir, err := startTransientService(systemdRun, command, 0, processIsolation{privateNetwork: true, noNewPrivileges: true, privateTmp: true})
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
	if !containsAdjacent(args, "-p", "PrivateNetwork=yes") || !containsAdjacent(args, "-p", "NoNewPrivileges=yes") || !containsAdjacent(args, "-p", "PrivateTmp=yes") {
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

	_, _, _, _, logDir, err := startTransientService(systemdRun, exec.Command("/bin/true"), 0, processIsolation{noNewPrivileges: true})
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
	if containsAdjacent(args, "-p", "PrivateNetwork=yes") || containsAdjacent(args, "-p", "PrivateTmp=yes") || containsAdjacent(args, "-p", "MemoryMax=0") {
		t.Fatalf("NNP-only 不应隐式启用其他限制: %q", args)
	}
}

func TestStartTransientServicePrivateTmpWithoutOtherIsolation(t *testing.T) {
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

	_, _, _, _, logDir, err := startTransientService(systemdRun, exec.Command("/bin/true"), 0, processIsolation{privateTmp: true})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(logDir)
	content, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Split(strings.TrimSpace(string(content)), "\n")
	if !containsAdjacent(args, "-p", "PrivateTmp=yes") {
		t.Fatalf("systemd-run 参数未启用 PrivateTmp: %q", args)
	}
	for _, property := range []string{"PrivateNetwork=yes", "NoNewPrivileges=yes", "MemoryMax=0"} {
		if containsAdjacent(args, "-p", property) {
			t.Fatalf("PrivateTmp-only 不应隐式启用 %s: %q", property, args)
		}
	}
}

func TestCreateTransientServiceLogDirWithPrivateTmpUsesPersistentDirectory(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	runtimeDir, err := os.MkdirTemp(home, ".botfabric-private-tmp-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(runtimeDir)
	t.Setenv("XDG_RUNTIME_DIR", runtimeDir)
	t.Setenv("HOME", home)
	t.Setenv("TMPDIR", t.TempDir())

	path, err := createTransientServiceLogDir(true)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)
	if !strings.HasPrefix(filepath.Base(path), ".botfabric-log-") {
		t.Fatalf("PrivateTmp 日志目录格式异常: %q", path)
	}
}

func TestCreateTransientServiceLogDirWithPrivateTmpRejectsTemporaryDirectory(t *testing.T) {
	for _, base := range []string{"/tmp", "/var/tmp"} {
		t.Run(base, func(t *testing.T) {
			t.Setenv("XDG_RUNTIME_DIR", base)
			t.Setenv("HOME", t.TempDir())
			if _, err := createTransientServiceLogDir(true); err == nil {
				t.Fatalf("PrivateTmp 不应把托管环境文件放入 %s", base)
			}
		})
	}
}

func TestStartTransientServiceSyscallFilterWithoutOtherIsolation(t *testing.T) {
	tmp := t.TempDir()
	binDir := filepath.Join(tmp, "bin")
	if err := os.Mkdir(binDir, 0o700); err != nil {
		t.Fatal(err)
	}
	argsPath := filepath.Join(tmp, "args")
	systemdRun := filepath.Join(binDir, "systemd-run")
	systemctl := filepath.Join(binDir, "systemctl")
	writeExecutable(t, systemdRun, `#!/bin/sh
printf '%s\n' "$@" > "$BOTFABRIC_TEST_ARGS"
`)
	writeExecutable(t, systemctl, `#!/bin/sh
printf '1\n'
`)
	t.Setenv("PATH", binDir)
	t.Setenv("BOTFABRIC_TEST_ARGS", argsPath)
	t.Setenv("TMPDIR", tmp)

	_, _, _, _, logDir, err := startTransientService(systemdRun, exec.Command("/bin/true"), 0, processIsolation{restrictSyscalls: true})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(logDir)
	content, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Split(strings.TrimSpace(string(content)), "\n")
	if !containsAdjacent(args, "-p", "SystemCallFilter=~@clock @debug @module @mount @obsolete @raw-io @reboot @swap @cpu-emulation") {
		t.Fatalf("systemd-run 参数未启用固定 syscall filter: %q", args)
	}
	for _, property := range []string{"PrivateNetwork=yes", "NoNewPrivileges=yes", "PrivateTmp=yes", "MemoryMax=0"} {
		if containsAdjacent(args, "-p", property) {
			t.Fatalf("syscall-filter-only 不应隐式启用 %s: %q", property, args)
		}
	}
}

func TestStartTransientServiceCapabilityBoundingSet(t *testing.T) {
	for _, test := range []struct {
		name  string
		set   []string
		value string
	}{
		{name: "allow-list", set: []string{"CAP_SYS_ADMIN", "CAP_NET_BIND_SERVICE"}, value: "CapabilityBoundingSet=CAP_SYS_ADMIN CAP_NET_BIND_SERVICE"},
		{name: "empty", set: []string{}, value: "CapabilityBoundingSet="},
	} {
		t.Run(test.name, func(t *testing.T) {
			tmp := t.TempDir()
			binDir := filepath.Join(tmp, "bin")
			if err := os.Mkdir(binDir, 0o700); err != nil {
				t.Fatal(err)
			}
			argsPath := filepath.Join(tmp, "args")
			systemdRun := filepath.Join(binDir, "systemd-run")
			systemctl := filepath.Join(binDir, "systemctl")
			writeExecutable(t, systemdRun, `#!/bin/sh
printf '%s\n' "$@" > "$BOTFABRIC_TEST_ARGS"
exit 0
`)
			writeExecutable(t, systemctl, `#!/bin/sh
printf '1\n'
`)
			t.Setenv("PATH", binDir)
			t.Setenv("BOTFABRIC_TEST_ARGS", argsPath)
			t.Setenv("TMPDIR", tmp)

			_, _, _, _, logDir, err := startTransientService(systemdRun, exec.Command("/bin/true"), 0, processIsolation{capabilityBoundingSet: test.set})
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(logDir)
			content, err := os.ReadFile(argsPath)
			if err != nil {
				t.Fatal(err)
			}
			args := strings.Split(strings.TrimSpace(string(content)), "\n")
			if !containsAdjacent(args, "-p", test.value) {
				t.Fatalf("systemd-run capability 属性缺失: %q", args)
			}
		})
	}
}

func TestStartTransientServiceRestrictsAddressFamilies(t *testing.T) {
	tmp := t.TempDir()
	binDir := filepath.Join(tmp, "bin")
	if err := os.Mkdir(binDir, 0o700); err != nil {
		t.Fatal(err)
	}
	argsPath := filepath.Join(tmp, "args")
	systemdRun := filepath.Join(binDir, "systemd-run")
	writeExecutable(t, systemdRun, `#!/bin/sh
printf '%s\n' "$@" > "$BOTFABRIC_TEST_ARGS"
exit 0
`)
	writeExecutable(t, filepath.Join(binDir, "systemctl"), `#!/bin/sh
printf '1\n'
`)
	t.Setenv("PATH", binDir)
	t.Setenv("BOTFABRIC_TEST_ARGS", argsPath)
	t.Setenv("TMPDIR", tmp)

	_, _, _, _, logDir, err := startTransientService(systemdRun, exec.Command("/bin/true"), 0, processIsolation{restrictAddressFamilies: true})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(logDir)
	content, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Split(strings.TrimSpace(string(content)), "\n")
	if !containsAdjacent(args, "-p", "RestrictAddressFamilies=AF_UNIX") {
		t.Fatalf("systemd-run 地址族限制属性缺失: %q", args)
	}
	for _, property := range []string{"PrivateNetwork=yes", "NoNewPrivileges=yes", "PrivateTmp=yes", "MemoryMax=0"} {
		if containsAdjacent(args, "-p", property) {
			t.Fatalf("地址族限制不应隐式启用 %s: %q", property, args)
		}
	}
}

func TestWaitServiceMainPIDWaitsForProcessAfterTransientUnitCreated(t *testing.T) {
	tmp := t.TempDir()
	binDir := filepath.Join(tmp, "bin")
	if err := os.Mkdir(binDir, 0o700); err != nil {
		t.Fatal(err)
	}
	firstCallPath := filepath.Join(tmp, "first-call")
	systemctl := filepath.Join(binDir, "systemctl")
	writeExecutable(t, systemctl, `#!/bin/sh
if [ ! -e "$BOTFABRIC_TEST_FIRST_CALL" ]; then
  : > "$BOTFABRIC_TEST_FIRST_CALL"
  printf '123456\n'
else
  printf '0\n'
fi
`)
	t.Setenv("PATH", binDir)
	t.Setenv("BOTFABRIC_TEST_FIRST_CALL", firstCallPath)

	start := time.Now()
	if _, err := waitServiceMainPID("test", 60*time.Millisecond); err == nil {
		t.Fatal("不存在的 transient service PID 不应被接受")
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Fatal("等待逻辑应继续轮询，直到主进程启动或超时")
	}
}

func TestStartCgroupIsolationFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name      string
		isolation processIsolation
	}{
		{name: "private-network", isolation: processIsolation{privateNetwork: true}},
		{name: "no-new-privileges", isolation: processIsolation{noNewPrivileges: true}},
		{name: "private-tmp", isolation: processIsolation{privateTmp: true}},
		{name: "restrict-syscalls", isolation: processIsolation{restrictSyscalls: true}},
		{name: "restrict-address-families", isolation: processIsolation{restrictAddressFamilies: true}},
		{name: "capability-bounding-set", isolation: processIsolation{capabilityBoundingSet: []string{}}},
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

			enforcer, err := startCgroupMemoryLimit(exec.Command("/bin/true"), 0, test.isolation, slog.New(slog.NewTextHandler(os.Stderr, nil)), "test.plugin")
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

	enforcer, err := startCgroupMemoryLimit(exec.Command("/bin/true"), 64*1024*1024, processIsolation{}, slog.New(slog.NewTextHandler(os.Stderr, nil)), "test.plugin")
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
