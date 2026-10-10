package builtin

import (
	"testing"

	"github.com/PhantomDaze/botfabric/internal/runtime"
	"github.com/PhantomDaze/botfabric/plugins/application/reply"
	"github.com/PhantomDaze/botfabric/plugins/classifier"
	"github.com/PhantomDaze/botfabric/plugins/protocol/mockonebot"
)

func TestRegisterAddsAllBuiltinFactories(t *testing.T) {
	loader := runtime.NewBuiltinLoader()
	if err := Register(loader); err != nil {
		t.Fatal(err)
	}
	for _, manifest := range []struct {
		name string
		load func() error
	}{
		{name: mockonebot.PluginID, load: func() error {
			_, err := loader.Load(mockonebot.New().Manifest(), nil)
			return err
		}},
		{name: reply.PluginID, load: func() error {
			_, err := loader.Load(reply.New().Manifest(), nil)
			return err
		}},
		{name: classifier.RealLLMID, load: func() error {
			_, err := loader.Load(classifier.NewRealLLMPlugin().Manifest(), nil)
			return err
		}},
	} {
		if err := manifest.load(); err != nil {
			t.Fatalf("加载内置插件 %s: %v", manifest.name, err)
		}
	}
}
