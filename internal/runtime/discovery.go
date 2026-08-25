package runtime

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/PhantomDaze/botfabric/internal/config"
	"github.com/PhantomDaze/botfabric/pkg/contract"
)

// DiscoverManifests 从目录中读取全部 YAML 插件清单。
func DiscoverManifests(directory string) ([]contract.Manifest, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("读取插件目录: %w", err)
	}
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		extension := strings.ToLower(filepath.Ext(entry.Name()))
		if extension == ".yaml" || extension == ".yml" {
			paths = append(paths, filepath.Join(directory, entry.Name()))
		}
	}
	sort.Strings(paths)

	manifests := make([]contract.Manifest, 0, len(paths))
	for _, path := range paths {
		manifest, err := config.LoadManifest(path)
		if err != nil {
			return nil, fmt.Errorf("加载 %s: %w", path, err)
		}
		manifests = append(manifests, manifest)
	}
	return manifests, nil
}
