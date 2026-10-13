//go:build linux

package process

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	cgroupSetupTimeout    = 5 * time.Second
	cgroupLogPollInterval = 100 * time.Millisecond
)

// startCgroupMemoryLimit 使用 systemd transient service 设置 cgroup v2 内存限制，
// 并可按需启用 PrivateNetwork、NoNewPrivileges、PrivateTmp、capability bounding set、
// 地址族限制和系统调用过滤。隔离请求失败时返回错误，
// 由调用方拒绝无隔离启动；只有内存限制失败时允许回退 RSS 监控。
func startCgroupMemoryLimit(command *exec.Cmd, bytes int64, isolation processIsolation, logger *slog.Logger, pluginID string) (*memoryEnforcer, error) {
	if command == nil {
		return nil, errors.New("外部插件启动命令不能为空")
	}
	if bytes <= 0 && !isolation.requested() {
		return nil, nil
	}
	systemdRun, err := exec.LookPath("systemd-run")
	if err != nil {
		if isolation.requested() {
			return nil, fmt.Errorf("找不到 systemd-run: %w", err)
		}
		return nil, nil
	}

	unit, pid, stdoutPath, stderrPath, logDir, err := startTransientService(systemdRun, command, bytes, isolation)
	if err != nil {
		if isolation.requested() {
			return nil, err
		}
		logger.Warn("以 transient service 启动内存受限插件失败，回退 RSS 监控", "plugin_id", pluginID, "error", err)
		return nil, nil
	}
	command.Process = &os.Process{Pid: pid}

	ctx, cancel := context.WithCancel(context.Background())
	var tailWG sync.WaitGroup
	tailWG.Add(2)
	go func() { defer tailWG.Done(); tailProcessLog(stdoutPath, logger, pluginID, "stdout", ctx) }()
	go func() { defer tailWG.Done(); tailProcessLog(stderrPath, logger, pluginID, "stderr", ctx) }()
	stopLogs := sync.OnceFunc(func() {
		cancel()
		tailWG.Wait()
		_ = os.RemoveAll(logDir)
	})

	logger.Info("外部插件已启动 systemd 托管", "plugin_id", pluginID, "memory_max_bytes", bytes, "private_network", isolation.privateNetwork, "no_new_privileges", isolation.noNewPrivileges, "private_tmp", isolation.privateTmp, "restrict_syscalls", isolation.restrictSyscalls, "restrict_address_families", isolation.restrictAddressFamilies, "capability_bounding_set", isolation.capabilityBoundingSet, "unit", unit)
	return &memoryEnforcer{
		stop:    stopLogs,
		managed: true,
		reaper:  transientReaper(unit),
	}, nil
}

// startTransientService 先设置 systemd cgroup 属性，再启动插件。
//
// stdout/stderr 追加到普通文件并由 tailProcessLog 转发。环境变量通过权限为
// 0600 的 EnvironmentFile 传递，避免 Session Token 出现在 systemd-run 参数中。
func startTransientService(systemdRun string, command *exec.Cmd, memoryBytes int64, isolation processIsolation) (unit string, pid int, stdoutPath, stderrPath, logDir string, err error) {
	logDir, err = createTransientServiceLogDir(isolation.privateTmp)
	if err != nil {
		return "", 0, "", "", "", err
	}
	stdoutPath = filepath.Join(logDir, "stdout")
	stderrPath = filepath.Join(logDir, "stderr")
	envPath := filepath.Join(logDir, "environment")
	if err := writeSystemdEnvironmentFile(envPath, command.Env); err != nil {
		_ = os.RemoveAll(logDir)
		return "", 0, "", "", "", err
	}

	unit = fmt.Sprintf("botfabric-%d-%d", os.Getpid(), time.Now().UnixNano())
	args := []string{
		"--user", "--collect", "--no-block", "--unit=" + unit,
		"-p", "KillMode=control-group",
		"-p", "StandardOutput=append:" + stdoutPath,
		"-p", "StandardError=append:" + stderrPath,
		"-p", "EnvironmentFile=" + envPath,
	}
	if memoryBytes > 0 {
		args = append(args,
			"-p", "MemoryMax="+strconv.FormatInt(memoryBytes, 10),
			"-p", "MemorySwapMax=0",
			"-p", "OOMPolicy=continue",
		)
	}
	if isolation.privateNetwork {
		args = append(args, "-p", "PrivateNetwork=yes")
	}
	if isolation.noNewPrivileges {
		args = append(args, "-p", "NoNewPrivileges=yes")
	}
	if isolation.privateTmp {
		args = append(args, "-p", "PrivateTmp=yes")
	}
	if isolation.restrictAddressFamilies {
		args = append(args, "-p", "RestrictAddressFamilies=AF_UNIX")
	}
	if isolation.capabilityBoundingSet != nil {
		args = append(args, "-p", "CapabilityBoundingSet="+strings.Join(isolation.capabilityBoundingSet, " "))
	}
	if isolation.restrictSyscalls {
		args = append(args, "-p", "SystemCallFilter=~@clock @debug @module @mount @obsolete @raw-io @reboot @swap @cpu-emulation")
	}
	args = append(args, "--", command.Path)
	args = append(args, command.Args[1:]...)
	run := exec.Command(systemdRun, args...)
	if output, runErr := run.CombinedOutput(); runErr != nil {
		_ = os.RemoveAll(logDir)
		return "", 0, "", "", "", fmt.Errorf("systemd-run: %w (%s)", runErr, strings.TrimSpace(string(output)))
	}

	pid, err = waitServiceMainPID(unit, cgroupSetupTimeout)
	if err != nil {
		_ = stopTransientService(unit)
		_ = os.RemoveAll(logDir)
		return "", 0, "", "", "", err
	}
	return unit, pid, stdoutPath, stderrPath, logDir, nil
}

func createTransientServiceLogDir(privateTmp bool) (string, error) {
	if !privateTmp {
		return os.MkdirTemp("", "botfabric-log-")
	}

	base := os.Getenv("XDG_RUNTIME_DIR")
	if base == "" {
		base = os.Getenv("HOME")
	}
	if base == "" {
		return "", errors.New("PrivateTmp 需要 XDG_RUNTIME_DIR 或 HOME 来存放 systemd 环境和日志文件")
	}
	info, err := os.Stat(base)
	if err != nil {
		return "", fmt.Errorf("检查 PrivateTmp 临时文件目录: %w", err)
	}
	if !info.IsDir() {
		return "", errors.New("PrivateTmp systemd 临时文件路径不是目录")
	}
	resolved, err := filepath.EvalSymlinks(base)
	if err != nil {
		return "", fmt.Errorf("解析 PrivateTmp 临时文件目录: %w", err)
	}
	for _, root := range []string{"/tmp", "/var/tmp"} {
		for _, candidate := range []string{base, resolved} {
			for _, boundary := range []string{root, resolveExistingPath(root)} {
				relative, relErr := filepath.Rel(boundary, candidate)
				if relErr == nil && (relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)))) {
					return "", errors.New("PrivateTmp systemd 环境和日志文件目录不能位于 /tmp 或 /var/tmp")
				}
			}
		}
	}
	return os.MkdirTemp(resolved, ".botfabric-log-")
}

func resolveExistingPath(path string) string {
	resolved, err := filepath.EvalSymlinks(path)
	if err == nil {
		return resolved
	}
	return path
}

// writeSystemdEnvironmentFile 安全写入 transient service 的环境文件。
func writeSystemdEnvironmentFile(path string, env []string) error {
	values := make(map[string]string, len(env))
	for _, entry := range env {
		name, value, ok := strings.Cut(entry, "=")
		if !ok || !validEnvironmentName(name) {
			return fmt.Errorf("无效的插件环境变量名 %q", name)
		}
		values[name] = value
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		_ = file.Close()
		if !ok {
			_ = os.Remove(path)
		}
	}()
	for _, name := range names {
		if _, err := fmt.Fprintf(file, "%s=\"%s\"\n", name, escapeSystemdEnvironmentValue(values[name])); err != nil {
			return err
		}
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	ok = true
	return nil
}

func validEnvironmentName(name string) bool {
	if name == "" || !((name[0] >= 'A' && name[0] <= 'Z') || (name[0] >= 'a' && name[0] <= 'z') || name[0] == '_') {
		return false
	}
	for i := 1; i < len(name); i++ {
		c := name[i]
		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') {
			return false
		}
	}
	return true
}

func escapeSystemdEnvironmentValue(value string) string {
	var escaped strings.Builder
	for i := 0; i < len(value); i++ {
		switch c := value[i]; c {
		case '\\', '"':
			escaped.WriteByte('\\')
			escaped.WriteByte(c)
		case '\n':
			escaped.WriteString("\\n")
		case '\r':
			escaped.WriteString("\\r")
		case '\t':
			escaped.WriteString("\\t")
		default:
			if c < 0x20 || c == 0x7f {
				_, _ = fmt.Fprintf(&escaped, "\\x%02x", c)
			} else {
				escaped.WriteByte(c)
			}
		}
	}
	return escaped.String()
}

// tailProcessLog 轮询普通日志文件的增量内容并按行转发。
func tailProcessLog(path string, logger *slog.Logger, pluginID, stream string, ctx context.Context) {
	var file *os.File
	for file == nil {
		f, err := os.Open(path)
		if err == nil {
			file = f
			break
		}
		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	for {
		line, readErr := reader.ReadString('\n')
		if len(line) > 0 {
			logProcessOutput(logger, pluginID, stream, line)
		}
		if readErr != nil {
			select {
			case <-ctx.Done():
				for {
					rest, err := reader.ReadString('\n')
					if rest != "" {
						logProcessOutput(logger, pluginID, stream, rest)
					}
					if err != nil {
						return
					}
				}
			default:
				time.Sleep(cgroupLogPollInterval)
			}
		}
	}
}

func logProcessOutput(logger *slog.Logger, pluginID, stream, line string) {
	message := strings.TrimRight(line, "\n")
	if len(message) > 4096 {
		message = message[:4096] + "…"
	}
	logger.Info("外部插件输出", "plugin_id", pluginID, "stream", stream, "message", message)
}

// transientReaper 返回终止 transient service 的函数。KillMode=control-group
// 确保服务 cgroup 内的所有后代一并回收；unit 已被 --collect 卸载时忽略错误。
func transientReaper(unit string) func() error {
	return func() error {
		_ = stopTransientService(unit)
		return nil
	}
}

func waitServiceMainPID(unit string, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := exec.Command("systemctl", "--user", "show", "-p", "MainPID", "--value", unit+".service").Output()
		if err == nil {
			if pid, convErr := strconv.Atoi(strings.TrimSpace(string(out))); convErr == nil && pid > 0 {
				if processErr := syscall.Kill(pid, 0); processErr == nil || errors.Is(processErr, syscall.EPERM) {
					return pid, nil
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return 0, errors.New("等待插件主进程 PID 超时")
}

func stopTransientService(unit string) error {
	return exec.Command("systemctl", "--user", "stop", unit+".service").Run()
}

// waitManagedProcess 轮询等待由 systemd 托管的插件进程退出。该进程不是
// Runtime 的子进程，os.Process.Wait 会返回 ECHILD，不能用于等待。
func waitManagedProcess(command *exec.Cmd) error {
	if command.Process == nil {
		return nil
	}
	for syscall.Kill(command.Process.Pid, 0) == nil {
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}
