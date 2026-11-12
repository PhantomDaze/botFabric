package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/PhantomDaze/botfabric/internal/config"
	"github.com/PhantomDaze/botfabric/internal/dedup"
	"github.com/PhantomDaze/botfabric/internal/eventbus"
	"github.com/PhantomDaze/botfabric/internal/gateway"
	"github.com/PhantomDaze/botfabric/internal/management"
	"github.com/PhantomDaze/botfabric/internal/observability"
	"github.com/PhantomDaze/botfabric/internal/onebotapp"
	"github.com/PhantomDaze/botfabric/internal/policy"
	"github.com/PhantomDaze/botfabric/internal/process"
	"github.com/PhantomDaze/botfabric/internal/registry"
	"github.com/PhantomDaze/botfabric/internal/response"
	"github.com/PhantomDaze/botfabric/internal/runtime"
	"github.com/PhantomDaze/botfabric/internal/store"
	"github.com/PhantomDaze/botfabric/pkg/contract"
	"github.com/PhantomDaze/botfabric/plugins/builtin"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	configPath := "config/runtime.example.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}
	ctx, stopSignal := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignal()
	return runWithContext(ctx, configPath, nil)
}

func runWithContext(ctx context.Context, configPath string, ready func()) error {
	runtimeConfig, err := config.Load(configPath)
	if err != nil {
		return err
	}
	logger, err := observability.NewLogger(os.Stderr, runtimeConfig.Observability.LogFormat, slog.LevelInfo)
	if err != nil {
		return err
	}
	checker, err := policy.Load(runtimeConfig.Policy.File)
	if err != nil {
		return err
	}
	if checker.DefaultDecision() != runtimeConfig.Policy.Default {
		return fmt.Errorf("runtime 配置与策略文件的 policy.default 不一致")
	}
	reg := registry.New()
	deadLetters := eventbus.NewMemoryDeadLetterSink(0)
	bus, err := eventbus.New(runtimeConfig.Runtime.DefaultHandlerTimeout.Duration, checker, logger, deadLetters)
	if err != nil {
		return err
	}
	serviceGateway, err := gateway.New(reg, checker, logger, runtimeConfig.Runtime.DefaultHandlerTimeout.Duration)
	if err != nil {
		return err
	}
	manager, err := runtime.NewManager(
		reg,
		bus,
		serviceGateway,
		logger,
		runtimeConfig.Runtime.BotID,
		runtimeConfig.Runtime.ShutdownTimeout.Duration,
	)
	if err != nil {
		return err
	}

	var metrics *observability.Metrics
	if runtimeConfig.Observability.MetricsEnabled {
		metrics = observability.NewMetrics()
		manager.SetMetrics(metrics)
	}

	// 分布式共享存储：启用 Redis 时去重与响应状态跨实例共享，进程死亡由 key TTL 兜底。
	// 未启用时两个子系统各自回退到进程内内存实现，无需外部依赖。
	var redisClient *store.Client
	if runtimeConfig.Redis.Enabled {
		redisClient, err = store.New(ctx, store.Config{
			Address:   runtimeConfig.Redis.Address,
			Password:  runtimeConfig.Redis.Password,
			DB:        runtimeConfig.Redis.DB,
			KeyPrefix: runtimeConfig.Redis.Prefix(),
		})
		if err != nil {
			return fmt.Errorf("连接 Redis 共享存储: %w", err)
		}
		dedupStore, derr := dedup.NewRedisStore(redisClient)
		if derr != nil {
			_ = redisClient.Close()
			return fmt.Errorf("创建 Redis 去重存储: %w", derr)
		}
		manager.SetDedupStore(dedupStore)
		responseStore, rerr := response.NewRedisStore(redisClient, 0)
		if rerr != nil {
			_ = redisClient.Close()
			return fmt.Errorf("创建 Redis 响应存储: %w", rerr)
		}
		if cerr := manager.ResponseCoordinator().SetStore(responseStore); cerr != nil {
			_ = redisClient.Close()
			return fmt.Errorf("注入 Redis 响应存储: %w", cerr)
		}
		logger.Info("已启用 Redis 共享存储", "address", runtimeConfig.Redis.Address)
	}

	loader := runtime.NewBuiltinLoader()
	if err := builtin.Register(loader); err != nil {
		return err
	}
	if runtimeConfig.OneBot.Enabled {
		if err := builtin.RegisterOneBot(loader, runtimeConfig.OneBot); err != nil {
			return err
		}
	}
	if err := manager.RegisterLoader(loader); err != nil {
		return err
	}
	processLoader, err := process.NewLoaderWithFactory(logger, runtimeConfig.Transport.UnixSocketDirectory, process.NewSessionIssuer(), func(manifest contract.Manifest) (process.Transport, error) {
		return process.NewGRPCTransport(process.GRPCTransportConfig{Logger: logger, MaxMessageSize: int64(runtimeConfig.Transport.MaxMessageSize)})
	})
	if err != nil {
		return err
	}
	// 外部插件进程异常退出时由 Runtime 回滚其服务/订阅并置为 failed。
	processLoader.SetFailureHandler(manager.NotifyPluginExit)
	if err := manager.RegisterLoader(processLoader); err != nil {
		return err
	}
	manifests, err := runtime.DiscoverManifests(runtimeConfig.Runtime.PluginDirectory)
	if err != nil {
		return err
	}
	for _, manifest := range manifests {
		if manifest.ID == "protocol.onebot" && !runtimeConfig.OneBot.Enabled {
			continue
		}
		if manifest.ID == "protocol.mockonebot" && runtimeConfig.OneBot.Enabled {
			continue
		}
		if err := manager.Load(manifest); err != nil {
			return err
		}
	}

	if err := manager.StartAll(ctx); err != nil {
		return err
	}
	var applicationOneBot *onebotapp.Adapter
	if runtimeConfig.ApplicationOneBot.Enabled {
		applicationOneBot, err = onebotapp.New(onebotapp.Config{
			Enabled:              true,
			Mode:                 runtimeConfig.ApplicationOneBot.Mode,
			ListenAddress:        runtimeConfig.ApplicationOneBot.ListenAddress,
			PathV11:              runtimeConfig.ApplicationOneBot.PathV11,
			PathV12:              runtimeConfig.ApplicationOneBot.PathV12,
			AccessToken:          runtimeConfig.ApplicationOneBot.AccessToken,
			ReadTimeout:          runtimeConfig.ApplicationOneBot.ReadTimeout.Duration,
			WriteTimeout:         runtimeConfig.ApplicationOneBot.WriteTimeout.Duration,
			HeartbeatInterval:    runtimeConfig.ApplicationOneBot.HeartbeatInterval.Duration,
			RemoteAddress:        runtimeConfig.ApplicationOneBot.RemoteAddress,
			RemotePath:           runtimeConfig.ApplicationOneBot.RemotePath,
			RemoteVersion:        runtimeConfig.ApplicationOneBot.RemoteVersion,
			ReconnectInterval:    runtimeConfig.ApplicationOneBot.ReconnectInterval.Duration,
			ReconnectMaxInterval: runtimeConfig.ApplicationOneBot.ReconnectMaxInterval.Duration,
			ReconnectMaxTries:    runtimeConfig.ApplicationOneBot.ReconnectMaxTries,
			MaxMessageSize:       int64(runtimeConfig.ApplicationOneBot.MaxMessageSize),
		}, manager.Endpoints(), manager.ResponseCoordinator())
		if err != nil {
			_ = manager.StopAll(context.Background())
			return err
		}
		if err := bus.RegisterSink(applicationOneBot); err != nil {
			_ = manager.StopAll(context.Background())
			return err
		}
		if err := applicationOneBot.Start(); err != nil {
			bus.UnregisterSink(applicationOneBot)
			_ = manager.StopAll(context.Background())
			return err
		}
		if runtimeConfig.ApplicationOneBot.Mode == "client" || runtimeConfig.ApplicationOneBot.Mode == "both" {
			if err := applicationOneBot.Connect(ctx, runtimeConfig.ApplicationOneBot.RemoteAddress, runtimeConfig.ApplicationOneBot.RemotePath, runtimeConfig.ApplicationOneBot.RemoteVersion); err != nil {
				bus.UnregisterSink(applicationOneBot)
				_ = applicationOneBot.Shutdown(context.Background())
				_ = manager.StopAll(context.Background())
				return fmt.Errorf("连接远端应用端 OneBot: %w", err)
			}
		}
	}
	var metricsServer *observability.MetricsServer
	if metrics != nil {
		metricsServer, err = observability.NewMetricsServer(runtimeConfig.Observability.MetricsListenAddress, metrics)
		if err != nil {
			_ = manager.StopAll(context.Background())
			return err
		}
		if err := metricsServer.Start(); err != nil {
			_ = manager.StopAll(context.Background())
			return err
		}
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), runtimeConfig.Runtime.ShutdownTimeout.Duration)
			defer cancel()
			_ = metricsServer.Shutdown(shutdownCtx)
		}()
		logger.Info("指标端点已启动", "address", metricsServer.Addr())
	}
	var managementServer *management.Server
	if runtimeConfig.Management.Enabled {
		managementServer, err = management.New(runtimeConfig.Management.ListenAddress, runtimeConfig.Management.BearerToken, manager, deadLetters)
		if err != nil {
			if metricsServer != nil {
				_ = metricsServer.Shutdown(context.Background())
			}
			_ = manager.StopAll(context.Background())
			return err
		}
		if err := managementServer.Start(); err != nil {
			if metricsServer != nil {
				_ = metricsServer.Shutdown(context.Background())
			}
			_ = manager.StopAll(context.Background())
			return err
		}
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), runtimeConfig.Runtime.ShutdownTimeout.Duration)
			defer cancel()
			_ = managementServer.Shutdown(shutdownCtx)
		}()
	}
	logger.Info("BotFabric 运行时已启动", "plugins", len(manifests), "bot_id", runtimeConfig.Runtime.BotID)
	if ready != nil {
		ready()
	}
	<-ctx.Done()
	logger.Info("收到停机信号")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), runtimeConfig.Runtime.ShutdownTimeout.Duration)
	defer cancel()
	if applicationOneBot != nil {
		bus.UnregisterSink(applicationOneBot)
		if err := applicationOneBot.Shutdown(shutdownCtx); err != nil {
			return err
		}
	}
	if err := manager.StopAll(shutdownCtx); err != nil {
		return err
	}
	if redisClient != nil {
		_ = redisClient.Close()
	}
	logger.Info("BotFabric 运行时已停止", "dead_letters", len(deadLetters.Entries()))
	return nil
}
