package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration 支持从 YAML 的 Go duration 字符串读取时长。
type Duration struct {
	time.Duration
}

// UnmarshalYAML 解析 500ms、2s 等时长格式。
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	value, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("无效时长 %q: %w", node.Value, err)
	}
	if value <= 0 {
		return errors.New("时长必须大于零")
	}
	d.Duration = value
	return nil
}

// ByteSize 支持从 YAML 读取 KiB、MiB 和 GiB 格式的字节数。
type ByteSize int64

// UnmarshalYAML 解析非负字节大小。
func (s *ByteSize) UnmarshalYAML(node *yaml.Node) error {
	value, err := parseByteSize(node.Value)
	if err != nil {
		return err
	}
	*s = ByteSize(value)
	return nil
}

func parseByteSize(input string) (int64, error) {
	units := []struct {
		suffix string
		factor int64
	}{
		{"GiB", 1024 * 1024 * 1024},
		{"MiB", 1024 * 1024},
		{"KiB", 1024},
		{"B", 1},
	}
	for _, unit := range units {
		if strings.HasSuffix(input, unit.suffix) {
			number := strings.TrimSpace(strings.TrimSuffix(input, unit.suffix))
			value, err := strconv.ParseInt(number, 10, 64)
			if err != nil || value < 0 {
				return 0, fmt.Errorf("无效字节大小 %q", input)
			}
			if value > (1<<63-1)/unit.factor {
				return 0, fmt.Errorf("字节大小溢出 %q", input)
			}
			return value * unit.factor, nil
		}
	}
	return 0, fmt.Errorf("字节大小 %q 缺少 B、KiB、MiB 或 GiB 单位", input)
}

// Config 是核心运行时配置。
type Config struct {
	Runtime           RuntimeConfig           `yaml:"runtime"`
	Transport         TransportConfig         `yaml:"transport"`
	Policy            PolicyConfig            `yaml:"policy"`
	Observability     ObservabilityConfig     `yaml:"observability"`
	OneBot            OneBotConfig            `yaml:"onebot"`
	ApplicationOneBot ApplicationOneBotConfig `yaml:"application_onebot"`
	Redis             RedisConfig             `yaml:"redis"`
	Management        ManagementConfig        `yaml:"management"`
}

// RuntimeConfig 控制插件目录与生命周期超时。
type RuntimeConfig struct {
	PluginDirectory       string   `yaml:"plugin_directory"`
	ShutdownTimeout       Duration `yaml:"shutdown_timeout"`
	DefaultHandlerTimeout Duration `yaml:"default_handler_timeout"`
	BotID                 string   `yaml:"bot_id"`
}

// TransportConfig 控制外部插件传输边界。
type TransportConfig struct {
	UnixSocketDirectory string   `yaml:"unix_socket_directory"`
	MaxMessageSize      ByteSize `yaml:"max_message_size"`
}

// PolicyConfig 指定权限策略来源和默认决策。
type PolicyConfig struct {
	File    string `yaml:"file"`
	Default string `yaml:"default"`
}

// ObservabilityConfig 控制日志、指标和追踪。
type ObservabilityConfig struct {
	LogFormat      string `yaml:"log_format"`
	MetricsEnabled bool   `yaml:"metrics_enabled"`
	TracingEnabled bool   `yaml:"tracing_enabled"`
}

// Load 以严格模式加载运行时配置。
func Load(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("打开运行时配置: %w", err)
	}
	defer file.Close()

	var config Config
	if err := decodeStrict(file, &config); err != nil {
		return Config{}, fmt.Errorf("解析运行时配置: %w", err)
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func decodeStrict(reader io.Reader, destination any) error {
	decoder := yaml.NewDecoder(reader)
	decoder.KnownFields(true)
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("配置只能包含一个 YAML 文档")
		}
		return err
	}
	return nil
}

// Validate 检查运行时配置的必填项和枚举值。
func (c Config) Validate() error {
	if c.Runtime.PluginDirectory == "" {
		return errors.New("runtime.plugin_directory 不能为空")
	}
	if c.Runtime.ShutdownTimeout.Duration <= 0 {
		return errors.New("runtime.shutdown_timeout 必须大于零")
	}
	if c.Runtime.DefaultHandlerTimeout.Duration <= 0 {
		return errors.New("runtime.default_handler_timeout 必须大于零")
	}
	if c.Runtime.BotID == "" {
		return errors.New("runtime.bot_id 不能为空")
	}
	if c.Transport.UnixSocketDirectory == "" {
		return errors.New("transport.unix_socket_directory 不能为空")
	}
	if c.Transport.MaxMessageSize <= 0 {
		return errors.New("transport.max_message_size 必须大于零")
	}
	if c.Policy.File == "" {
		return errors.New("policy.file 不能为空")
	}
	if c.Policy.Default != "deny" && c.Policy.Default != "allow" {
		return errors.New("policy.default 仅支持 deny 或 allow")
	}
	if c.Observability.LogFormat != "json" && c.Observability.LogFormat != "text" {
		return errors.New("observability.log_format 仅支持 json 或 text")
	}
	if err := c.OneBot.Validate(); err != nil {
		return err
	}
	if err := c.ApplicationOneBot.Validate(); err != nil {
		return err
	}
	if err := c.Redis.Validate(); err != nil {
		return err
	}
	if err := c.Management.Validate(); err != nil {
		return err
	}
	return nil
}
