// Command security_check enforces ChronicleGate's repository security policy.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

var (
	secretPatterns = []*regexp.Regexp{
		regexp.MustCompile(`AKIA[0-9A-Z]{16}`),
		regexp.MustCompile(`ghp_[A-Za-z0-9]{36}`),
		regexp.MustCompile(`-----BEGIN ` + `(?:RSA |EC |OPENSSH )?PRIVATE KEY-----`),
	}
)

type dependencyLock struct {
	SchemaVersion string             `json:"schemaVersion"`
	ReviewedAt    string             `json:"reviewedAt"`
	Sources       map[string]string  `json:"sources"`
	Dependencies  []lockedDependency `json:"dependencies"`
}

type lockedDependency struct {
	Name       string   `json:"name"`
	Version    string   `json:"version"`
	Kind       string   `json:"kind"`
	Advisories []string `json:"advisories"`
}

type moduleDependency struct {
	Version string
	Direct  bool
}

func main() {
	if err := checkRepository("."); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("repository security policy: PASS")
}

func checkRepository(root string) error {
	checks := []func(string) error{checkDependencyLock, checkSecrets, checkImageLock}
	var failures []error
	for _, check := range checks {
		if err := check(root); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func checkDependencyLock(root string) error {
	modules, goVersion, err := parseGoMod(filepath.Join(root, "go.mod"))
	if err != nil {
		return err
	}
	document, err := os.ReadFile(filepath.Join(root, "config", "dependencies.lock.json"))
	if err != nil {
		return fmt.Errorf("read dependency lock: %w", err)
	}
	var lock dependencyLock
	decoder := json.NewDecoder(strings.NewReader(string(document)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&lock); err != nil {
		return fmt.Errorf("decode dependency lock: %w", err)
	}
	seen := map[string]lockedDependency{}
	for _, dependency := range lock.Dependencies {
		if dependency.Name == "" || dependency.Version == "" || dependency.Kind == "" {
			return fmt.Errorf("dependency lock contains an incomplete entry")
		}
		if _, exists := seen[dependency.Name]; exists {
			return fmt.Errorf("dependency lock duplicates %s", dependency.Name)
		}
		seen[dependency.Name] = dependency
		switch dependency.Kind {
		case "module", "indirect-module":
			module, exists := modules[dependency.Name]
			if !exists {
				return fmt.Errorf("dependency lock has stale module %s", dependency.Name)
			}
			if module.Version != dependency.Version {
				return fmt.Errorf("dependency lock version for %s is %s, want %s", dependency.Name, dependency.Version, module.Version)
			}
			wantKind := "indirect-module"
			if module.Direct {
				wantKind = "module"
			}
			if dependency.Kind != wantKind {
				return fmt.Errorf("dependency lock kind for %s is %s, want %s", dependency.Name, dependency.Kind, wantKind)
			}
		case "tool", "toolchain":
		default:
			return fmt.Errorf("dependency lock has unsupported kind %q", dependency.Kind)
		}
	}
	for name, module := range modules {
		if !module.Direct {
			continue
		}
		dependency, exists := seen[name]
		if !exists || dependency.Version != module.Version || dependency.Kind != "module" {
			return fmt.Errorf("direct module %s is not exactly represented in dependency lock", name)
		}
	}
	toolchain, exists := seen["Go"]
	if !exists || toolchain.Kind != "toolchain" || toolchain.Version != goVersion {
		return fmt.Errorf("go %s is not exactly represented in dependency lock", goVersion)
	}
	return nil
}

func parseGoMod(path string) (map[string]moduleDependency, string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, "", fmt.Errorf("open go.mod: %w", err)
	}
	defer func() { _ = file.Close() }()
	modules := map[string]moduleDependency{}
	goVersion := ""
	inRequire := false
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "go ") {
			goVersion = strings.TrimSpace(strings.TrimPrefix(line, "go "))
			continue
		}
		if line == "require (" {
			inRequire = true
			continue
		}
		if inRequire && line == ")" {
			inRequire = false
			continue
		}
		if !inRequire || line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return nil, "", fmt.Errorf("malformed go.mod require line %q", line)
		}
		modules[fields[0]] = moduleDependency{Version: strings.TrimPrefix(fields[1], "v"), Direct: !strings.Contains(line, "// indirect")}
	}
	if err := scanner.Err(); err != nil {
		return nil, "", err
	}
	if goVersion == "" {
		return nil, "", fmt.Errorf("go.mod omits the Go version")
	}
	return modules, goVersion, nil
}

func checkSecrets(root string) error {
	return filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			name := info.Name()
			if path != root && (strings.HasPrefix(name, ".git") || name == "bin" || name == "dist" || name == "run") {
				return filepath.SkipDir
			}
			return nil
		}
		if !info.Mode().IsRegular() || info.Size() > 4<<20 {
			return nil
		}
		document, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, pattern := range secretPatterns {
			if pattern.Find(document) != nil {
				return fmt.Errorf("possible secret material in %s", path)
			}
		}
		return nil
	})
}

func checkImageLock(root string) error {
	document, err := os.ReadFile(filepath.Join(root, "config", "images.lock.json"))
	if err != nil {
		return fmt.Errorf("read image lock: %w", err)
	}
	var value struct {
		Images []struct {
			Name        string            `json:"name"`
			Role        string            `json:"role"`
			Reference   string            `json:"reference"`
			IndexDigest string            `json:"indexDigest"`
			Platforms   map[string]string `json:"platforms"`
			Hardening   *struct {
				CapDrop []string            `json:"capDrop"`
				CapAdd  map[string][]string `json:"capAdd"`
			} `json:"hardening"`
		} `json:"images"`
	}
	if err := json.Unmarshal(document, &value); err != nil {
		return fmt.Errorf("decode image lock: %w", err)
	}
	names := []string{}
	for _, image := range value.Images {
		names = append(names, image.Name)
		if !strings.Contains(image.Reference, "@sha256:") || !validDigest(image.IndexDigest) {
			return fmt.Errorf("image %s is not pinned by an OCI index digest", image.Name)
		}
		for _, platform := range []string{"linux/amd64", "linux/arm64"} {
			if !validDigest(image.Platforms[platform]) {
				return fmt.Errorf("image %s has no valid %s child digest", image.Name, platform)
			}
			if image.Role == "runtime" && (image.Hardening == nil || len(image.Hardening.CapDrop) != 1 || image.Hardening.CapDrop[0] != "ALL" || image.Hardening.CapAdd[platform] == nil) {
				return fmt.Errorf("runtime image %s has no exact %s hardening policy", image.Name, platform)
			}
		}
	}
	sort.Strings(names)
	return nil
}

func validDigest(value string) bool {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, character := range strings.TrimPrefix(value, "sha256:") {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}
