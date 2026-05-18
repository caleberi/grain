package nexus

import (
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"

	"github.com/gobwas/glob"
)

type PluginBuilder struct {
	glob      glob.Glob
	errChan   chan error
	filePaths chan string
	dirPath   string
}

func NewPluginBuilder(dirPath string, fileGlob string) (*PluginBuilder, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("failed to get current directory: %s", cwd)
	}

	stat, err := os.Lstat(filepath.Clean(filepath.Join(cwd, dirPath)))
	if err != nil {
		return nil, fmt.Errorf("failed to stat directory %s: %v", dirPath, err)
	}

	if !stat.IsDir() {
		return nil, fmt.Errorf("the specified path %s is not a directory", dirPath)
	}

	gb, err := glob.Compile(fileGlob)
	if err != nil {
		return nil, fmt.Errorf("invalid glob pattern %s: %v", fileGlob, err)
	}

	return &PluginBuilder{
		dirPath:   dirPath,
		glob:      gb,
		errChan:   make(chan error, 1),
		filePaths: make(chan string, 1),
	}, nil
}

func (pb *PluginBuilder) walker(path string, d fs.DirEntry, err error) error {
	if err != nil {
		pb.errChan <- err
		return err
	}

	if d.IsDir() {
		return nil
	}

	if d.Type().IsRegular() && pb.glob.Match(d.Name()) {
		pb.filePaths <- path
	}
	return nil
}

func (pb *PluginBuilder) LocateAndBuildPlugins(outputDir string) error {
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get current directory: %s", cwd)
	}

	outputDir = filepath.Clean(filepath.Join(cwd, outputDir))
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory %s: %v", outputDir, err)
	}

	go func() {
		defer close(pb.filePaths)
		defer close(pb.errChan)
		if err := fs.WalkDir(os.DirFS(pb.dirPath), ".", pb.walker); err != nil {
			pb.errChan <- err
		}
	}()

	pluginDirs := make(map[string]bool)
	for path := range pb.filePaths {
		pluginDir := filepath.Dir(path)
		pluginDirs[pluginDir] = true
	}

	select {
	case err := <-pb.errChan:
		if err != nil {
			return fmt.Errorf("error during directory walk: %v", err)
		}
	default:
	}

	var buildErrors []error
	goBinary := resolveGoBinary()
	buildFlags := extractBuildFlags()
	buildEnv := extractBuildEnv()

	for pluginDir := range pluginDirs {
		absPluginDir := filepath.Join(pb.dirPath, pluginDir)
		dirName := filepath.Base(pluginDir)
		if dirName == "." {
			dirName = filepath.Base(pb.dirPath)
		}
		outputFile := filepath.Join(outputDir, dirName+".so")
		args := []string{"build", "-buildmode=plugin", "-o", outputFile}
		args = append(args, buildFlags...)
		args = append(args, ".")

		cmd := exec.Command(goBinary, args...)
		cmd.Dir = absPluginDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if len(buildEnv) > 0 {
			cmd.Env = append(os.Environ(), buildEnv...)
		}

		if err := cmd.Run(); err != nil {
			buildErrors = append(buildErrors, fmt.Errorf("failed to build plugin in %s: %v", absPluginDir, err))
		}
	}

	if len(buildErrors) > 0 {
		return fmt.Errorf("encountered %d build errors: %v", len(buildErrors), buildErrors)
	}

	return nil
}

func resolveGoBinary() string {
	goBinary := "go"
	output, err := exec.Command("go", "env", "GOROOT").Output()
	if err != nil {
		return goBinary
	}
	goroot := strings.TrimSpace(string(output))
	if goroot == "" {
		return goBinary
	}
	candidate := filepath.Join(goroot, "bin", "go")
	if _, statErr := os.Stat(candidate); statErr != nil {
		return goBinary
	}
	return candidate
}

func isRaceBuild() bool {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return false
	}
	for _, setting := range info.Settings {
		if setting.Key == "race" && setting.Value == "true" {
			return true
		}
	}
	return false
}

// extractBuildFlags reconstructs the build flags used to build the current binary
// from runtime/debug.BuildInfo. Returns a slice of flag strings suitable for `go build`.
// Only includes actual command-line flags (like -race), not environment variables.
func extractBuildFlags() []string {
	var flags []string
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return flags
	}

	for _, setting := range info.Settings {
		switch setting.Key {
		case "race":
			if setting.Value == "true" {
				flags = append(flags, "-race")
			}
		case "cgo":
			if setting.Value == "0" {
				flags = append(flags, "-no-cgo")
			}
		}
	}

	return flags
}

// extractBuildEnv reconstructs the environment variables from runtime/debug.BuildInfo
// that should be set when building plugins. Returns KEY=VALUE strings for cmd.Env.
func extractBuildEnv() []string {
	var env []string
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return env
	}

	for _, setting := range info.Settings {
		switch setting.Key {
		case "CGO_ENABLED":
			env = append(env, "CGO_ENABLED="+setting.Value)
		case "CGO_CFLAGS", "CGO_CPPFLAGS", "CGO_CXXFLAGS", "CGO_LDFLAGS":
			if setting.Value != "" {
				env = append(env, setting.Key+"="+setting.Value)
			}
		}
	}

	return env
}
