package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"grain/nexus"
	"io/fs"
	"log"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

var (
	pluginStoreMu sync.RWMutex
	pluginStore   = make(map[string]PluginMetadata)
	reloadMu      sync.Mutex
	lastReloadAt  time.Time
)

type PluginMetadata struct {
	Name         string `json:"name"`
	Description  string `json:"description"`
	Code         string `json:"code"`
	CompiledPath string `json:"compiledPath"`
	CreatedAt    int64  `json:"createdAt"`
	CompiledAt   int64  `json:"compiledAt"`
}

const (
	pluginsDir     = "./cmd/web/plugins"
	pluginJSONFile = "plugin.json"
)

func init() {
	if err := os.MkdirAll(pluginsDir, 0755); err != nil {
		log.Printf("ERROR: creating plugins dir: %v", err)
		return
	}

	if err := loadPluginsFromDisk(); err != nil {
		log.Printf("ERROR: loading plugins from disk: %v", err)
	}
}

func storePluginCode(name, code, description string) error {
	now := getCurrentTimestamp()

	pluginStoreMu.Lock()
	existing, exists := pluginStore[name]

	createdAt := now
	if exists && existing.CreatedAt != 0 {
		createdAt = existing.CreatedAt
	}

	plugin := PluginMetadata{
		Name:         name,
		Code:         code,
		Description:  description,
		CreatedAt:    createdAt,
		CompiledAt:   0,
		CompiledPath: "",
	}

	pluginStore[name] = plugin
	pluginStoreMu.Unlock()

	if err := persistPluginToDisk(plugin); err != nil {
		return fmt.Errorf("failed to persist plugin metadata: %w", err)
	}

	return nil
}

func getPluginCode(name string) (string, error) {
	pluginStoreMu.RLock()
	defer pluginStoreMu.RUnlock()

	if plugin, exists := pluginStore[name]; exists {
		return plugin.Code, nil
	}
	return "", fmt.Errorf("plugin not found")
}

func listStoredPlugins() []map[string]any {
	pluginStoreMu.RLock()
	defer pluginStoreMu.RUnlock()

	var plugins []map[string]any
	for _, plugin := range pluginStore {
		plugins = append(plugins, map[string]any{
			"name":         plugin.Name,
			"description":  plugin.Description,
			"createdAt":    plugin.CreatedAt,
			"compiledAt":   plugin.CompiledAt,
			"isCompiled":   plugin.CompiledPath != "",
			"compiledPath": plugin.CompiledPath,
		})
	}

	go reloadPluginsIfNeeded(5 * time.Second)

	return plugins
}

func reloadPluginsIfNeeded(minInterval time.Duration) {
	reloadMu.Lock()
	defer reloadMu.Unlock()

	if !lastReloadAt.IsZero() && time.Since(lastReloadAt) < minInterval {
		return
	}

	if err := reloadPlugins(); err != nil {
		log.Printf("ERROR: reloading plugins from disk: %v", err)
		return
	}

	lastReloadAt = time.Now()
}

func compilePluginCode(name, code string) (string, error) {
	tmpDir := filepath.Join(pluginsDir, name)
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create plugin dir: %w", err)
	}

	mainFile := filepath.Join(tmpDir, "main.go")
	if err := os.WriteFile(mainFile, []byte(code), 0644); err != nil {
		return "", fmt.Errorf("failed to write plugin code: %w", err)
	}

	now := getCurrentTimestamp()
	soPath := filepath.Join(tmpDir, fmt.Sprintf("%s_%d.so", name, now))
	cmd := exec.Command("go", "build", "-buildmode=plugin", "-o", soPath, mainFile)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("compilation failed: %s", stderr.String())
	}

	pluginStoreMu.Lock()
	plugin, exists := pluginStore[name]
	if !exists {
		plugin = PluginMetadata{
			Name:      name,
			Code:      code,
			CreatedAt: now,
		}
	} else {
		plugin.Code = code
	}
	plugin.CompiledAt = now
	plugin.CompiledPath = soPath
	pluginStore[name] = plugin
	pluginStoreMu.Unlock()

	if err := persistPluginToDisk(plugin); err != nil {
		return "", fmt.Errorf("compiled but failed to persist metadata: %w", err)
	}

	if err := loadAndRegisterPlugin(soPath, name); err != nil {
		return "", fmt.Errorf("WARNING: compiled plugin %s but failed to register in core: %v", name, err)
	}

	return soPath, nil
}

func loadAndRegisterPlugin(soPath, name string) error {
	if nexusCore == nil {
		return fmt.Errorf("nexus core not initialized — plugin %s compiled but not registered", name)
	}

	p, err := nexus.LoadPlugin(soPath)
	if err != nil {
		return fmt.Errorf("failed to load plugin %s from %s: %w", name, soPath, err)
	}

	if err := nexusCore.RegisterPlugin(name, p); err != nil {
		return fmt.Errorf("failed to register plugin %s in core: %w", name, err)
	}

	log.Printf("INFO: plugin %s loaded and registered in NexusCore", name)
	return nil
}

func reloadPlugins() error {
	walkfunc := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			log.Printf("ERROR: walk plugin dir %s: %v", path, err)
			return nil
		}

		if !d.IsDir() || path == pluginsDir {
			return nil
		}

		name := filepath.Base(path)
		mainFile := filepath.Join(path, "main.go")
		soPath := filepath.Join(path, fmt.Sprintf("%s.so", name))

		mainInfo, err := os.Stat(mainFile)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			log.Printf("ERROR: stat main.go for %s: %v", name, err)
			return nil
		}

		if soInfo, statErr := os.Stat(soPath); statErr == nil {
			if !mainInfo.ModTime().After(soInfo.ModTime()) {
				return nil
			}
		}

		code, err := os.ReadFile(mainFile)
		if err != nil {
			log.Printf("ERROR: read plugin source for %s: %v", name, err)
			return nil
		}

		if _, err = compilePluginCode(name, string(code)); err != nil {
			log.Printf("ERROR: compile plugin %s: %v", name, err)
		}

		return nil
	}

	if err := filepath.WalkDir(pluginsDir, walkfunc); err != nil {
		return err
	}
	return nil
}

func persistPluginToDisk(plugin PluginMetadata) error {
	if plugin.Name == "" {
		return fmt.Errorf("plugin name is required")
	}

	dir := filepath.Join(pluginsDir, plugin.Name)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	mainFile := filepath.Join(dir, "main.go")
	if plugin.Code != "" {
		if err := os.WriteFile(mainFile, []byte(plugin.Code), 0644); err != nil {
			return err
		}
	}

	data, err := json.MarshalIndent(plugin, "", "  ")
	if err != nil {
		return err
	}

	tmp := filepath.Join(dir, pluginJSONFile+".tmp")
	dst := filepath.Join(dir, pluginJSONFile)

	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}

func loadPluginsFromDisk() error {
	entries, err := os.ReadDir(pluginsDir)
	if err != nil {
		return err
	}

	loaded := make(map[string]PluginMetadata)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		name := entry.Name()
		jsonPath := filepath.Join(pluginsDir, name, pluginJSONFile)

		data, readErr := os.ReadFile(jsonPath)
		if readErr != nil {
			if os.IsNotExist(readErr) {
				continue
			}
			log.Printf("ERROR: reading plugin json for %s: %v", name, readErr)
			continue
		}

		var plugin PluginMetadata
		if unmarshalErr := json.Unmarshal(data, &plugin); unmarshalErr != nil {
			log.Printf("ERROR: parsing plugin json for %s: %v", name, unmarshalErr)
			continue
		}

		if plugin.Name == "" {
			plugin.Name = name
		}

		loaded[plugin.Name] = plugin
	}

	pluginStoreMu.Lock()
	maps.Copy(pluginStore, loaded)
	pluginStoreMu.Unlock()

	return nil
}

func getCurrentTimestamp() int64 {
	return time.Now().Unix()
}
