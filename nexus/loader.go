package nexus

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"plugin"
	"sync"
)

func LoadPlugin(file string) (Plugin, error) {
	return loadPlugin(file)
}

func loadPlugin(file string) (Plugin, error) {
	pkg, err := plugin.Open(file)
	if err != nil {
		return nil, fmt.Errorf("failed to open plugin at %s: %w", file, err)
	}

	pluginSym, err := pkg.Lookup("Plugin")
	if err != nil {
		return nil, fmt.Errorf(`%s pkg.Lookup("Plugin"): %w`, file, err)
	}

	pluginFunc, ok := pluginSym.(func(string) (Plugin, error))
	if !ok {
		return nil, fmt.Errorf("invalid 'Plugin' symbol of '%s' plugin", file)
	}

	return pluginFunc(filepath.Base(file))
}

type PluginLoader struct {
	plugins    map[string]Plugin
	pluginPath string
	mu         sync.RWMutex
}

func NewPluginLoader(pluginPath string) (*PluginLoader, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("failed to get current working directory: %w", err)
	}

	return &PluginLoader{
		pluginPath: filepath.Join(cwd, pluginPath),
		plugins:    make(map[string]Plugin),
	}, nil
}

func (pl *PluginLoader) GetPlugins() map[string]Plugin {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	return pl.plugins
}

func (pl *PluginLoader) LoadPlugins() error {
	walkfunc := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || d.Name() == "go.mod" || d.Name() == "go.sum" {
			return nil
		}

		if d.Name() != "" && filepath.Ext(d.Name()) == ".so" {
			plugin, err := loadPlugin(path)
			if err != nil {
				return err
			}

			pl.mu.Lock()
			pl.plugins[plugin.Meta().Name] = plugin
			pl.mu.Unlock()
		}
		return nil
	}
	return filepath.WalkDir(pl.pluginPath, walkfunc)
}
