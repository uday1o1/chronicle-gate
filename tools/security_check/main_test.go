package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSecurityCheckRejectsFloatingActionAndSecret(t *testing.T) {
	root := testRepository(t)
	workflow := filepath.Join(root, ".github", "workflows", "ci.yml")
	if err := os.WriteFile(workflow, []byte("on: [pull_request]\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v7\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := checkWorkflows(root); err == nil || !strings.Contains(err.Error(), "not pinned") {
		t.Fatalf("floating action error = %v", err)
	}
	secret := "AKIA" + strings.Repeat("A", 16)
	if err := os.WriteFile(filepath.Join(root, "leak.txt"), []byte(secret), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := checkSecrets(root); err == nil {
		t.Fatal("secret scan accepted a credential-shaped value")
	}
}

func TestDependencyLockIsBidirectional(t *testing.T) {
	root := testRepository(t)
	if err := checkDependencyLock(root); err != nil {
		t.Fatal(err)
	}
	lock := `{"dependencies":[{"name":"Go","version":"1.26.6","kind":"toolchain"},{"name":"example.test/module","version":"1.2.4","kind":"module"}]}`
	if err := os.WriteFile(filepath.Join(root, "config", "dependencies.lock.json"), []byte(lock), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := checkDependencyLock(root); err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("mismatched lock error = %v", err)
	}
}

func testRepository(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, directory := range []string{"config", filepath.Join(".github", "workflows")} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	goMod := "module example.test/project\n\ngo 1.26.6\n\nrequire (\n\texample.test/module v1.2.3\n)\n"
	lock := `{"dependencies":[{"name":"Go","version":"1.26.6","kind":"toolchain"},{"name":"example.test/module","version":"1.2.3","kind":"module"}]}`
	workflow := "on: [pull_request]\njobs:\n  review:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/dependency-review-action@" + strings.Repeat("a", 40) + "\n"
	imageLock := `{"images":[{"name":"image","role":"runtime","reference":"example.test/image@sha256:` + strings.Repeat("a", 64) + `","indexDigest":"sha256:` + strings.Repeat("a", 64) + `","platforms":{"linux/amd64":"sha256:` + strings.Repeat("b", 64) + `","linux/arm64":"sha256:` + strings.Repeat("c", 64) + `"},"hardening":{"capDrop":["ALL"],"capAdd":{"linux/amd64":[],"linux/arm64":[]}}}]}`
	for path, document := range map[string]string{
		"go.mod": goMod, filepath.Join("config", "dependencies.lock.json"): lock,
		filepath.Join("config", "images.lock.json"): imageLock, filepath.Join(".github", "workflows", "ci.yml"): workflow,
	} {
		if err := os.WriteFile(filepath.Join(root, path), []byte(document), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}
