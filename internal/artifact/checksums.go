package artifact

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// WriteChecksums records every immutable regular artifact except explicit exclusions.
func WriteChecksums(root string, excluded map[string]struct{}) error {
	paths := []string{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if info.IsDir() && (relative == ".secrets" || strings.HasPrefix(relative, ".secrets/")) {
			return filepath.SkipDir
		}
		if info.Mode().IsRegular() {
			if _, skip := excluded[relative]; !skip {
				paths = append(paths, relative)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("inventory artifacts: %w", err)
	}
	sort.Strings(paths)
	var output strings.Builder
	for _, relative := range paths {
		document, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relative)))
		if err != nil {
			return fmt.Errorf("hash artifact %q: %w", relative, err)
		}
		digest := sha256.Sum256(document)
		fmt.Fprintf(&output, "%s  %s\n", hex.EncodeToString(digest[:]), relative)
	}
	return WriteFile(filepath.Join(root, "checksums.sha256"), []byte(output.String()))
}

// VerifyChecksums verifies the private run artifact inventory.
func VerifyChecksums(root string) error {
	file, err := os.Open(filepath.Join(root, "checksums.sha256"))
	if err != nil {
		return fmt.Errorf("open artifact checksums: %w", err)
	}
	defer func() { _ = file.Close() }()
	seen := map[string]struct{}{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "  ", 2)
		if len(parts) != 2 || len(parts[0]) != 64 {
			return fmt.Errorf("invalid artifact checksum line")
		}
		name := parts[1]
		if name == "" || strings.Contains(name, "\\") || filepath.IsAbs(name) || filepath.ToSlash(filepath.Clean(name)) != name || name == "events.ndjson" || name == "checksums.sha256" {
			return fmt.Errorf("unsafe artifact checksum path %q", name)
		}
		if _, exists := seen[name]; exists {
			return fmt.Errorf("duplicate artifact checksum path %q", name)
		}
		seen[name] = struct{}{}
		document, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(name)))
		if err != nil {
			return fmt.Errorf("read checksummed artifact %q: %w", name, err)
		}
		digest := sha256.Sum256(document)
		if hex.EncodeToString(digest[:]) != parts[0] {
			return fmt.Errorf("artifact checksum mismatch for %q", name)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read artifact checksums: %w", err)
	}
	return filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root || info.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if relative == "events.ndjson" || relative == "checksums.sha256" || strings.HasPrefix(relative, ".secrets/") {
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("artifact %q is not a regular file", relative)
		}
		if _, exists := seen[relative]; !exists {
			return fmt.Errorf("artifact %q is absent from checksum inventory", relative)
		}
		return nil
	})
}
