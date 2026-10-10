package builtin

import (
	"github.com/PhantomDaze/botfabric/internal/config"
	"github.com/PhantomDaze/botfabric/internal/runtime"
	"github.com/PhantomDaze/botfabric/pkg/contract"
	"github.com/PhantomDaze/botfabric/plugins/application/reply"
	"github.com/PhantomDaze/botfabric/plugins/classifier"
	"github.com/PhantomDaze/botfabric/plugins/protocol/mockonebot"
	"github.com/PhantomDaze/botfabric/plugins/protocol/onebot"
	"github.com/PhantomDaze/botfabric/plugins/routing"
)

// Register 将随服务端二进制编译的基础插件工厂注册到同进程加载器。
func Register(loader *runtime.BuiltinLoader) error {
	factories := []struct {
		entry   string
		factory contract.PluginFactory
	}{
		{entry: "builtin.mockonebot", factory: func() contract.Plugin { return mockonebot.New() }},
		{entry: "builtin.reply", factory: func() contract.Plugin { return reply.New() }},
		{entry: classifier.MockEntry, factory: func() contract.Plugin {
			return classifier.NewPlugin(contract.DispatchPlan{Mode: contract.DispatchBroadcast})
		}},
		{entry: classifier.RealLLMEntry, factory: func() contract.Plugin {
			return classifier.NewRealLLMPlugin()
		}},
		{entry: routing.BroadcastEntry, factory: func() contract.Plugin { return routing.New(routing.BroadcastManifest()) }},
		{entry: routing.RegexEntry, factory: func() contract.Plugin { return routing.New(routing.RegexManifest()) }},
		{entry: routing.CompetitionEntry, factory: func() contract.Plugin { return routing.New(routing.CompetitionManifest()) }},
		{entry: routing.RequiredBotEntry, factory: func() contract.Plugin { return routing.New(routing.RequiredBotManifest()) }},
	}
	for _, item := range factories {
		if err := loader.RegisterFactory(item.entry, item.factory); err != nil {
			return err
		}
	}
	return nil
}

// RegisterOneBot 注册可选的真实 OneBot v11/v12 协议插件。
func RegisterOneBot(loader *runtime.BuiltinLoader, oneBotConfig config.OneBotConfig) error {
	return loader.RegisterFactory("builtin.onebot", func() contract.Plugin { return onebot.New(oneBotConfig) })
}
