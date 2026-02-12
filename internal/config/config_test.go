/*
Copyright 2026 The pgmq-cli Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package config

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFromPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := `{
  "defaultServer": "DevServer",
  "servers": {
    "DevServer": {
      "connectionString": "Host=localhost;Username=postgres;Password=postgres;Database=pgmq;"
    }
  }
}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadFromPath(path)
	if err != nil {
		t.Fatalf("LoadFromPath error: %v", err)
	}
	if cfg.DefaultServer != "DevServer" {
		t.Fatalf("expected defaultServer DevServer, got %q", cfg.DefaultServer)
	}
	entry, ok := cfg.Servers["DevServer"]
	if !ok {
		t.Fatalf("expected DevServer entry")
	}
	if entry.ConnectionString == "" {
		t.Fatalf("expected connection string")
	}
}

func TestLoadFromPathMissing(t *testing.T) {
	_, err := LoadFromPath(filepath.Join(t.TempDir(), "missing.json"))
	if !errors.Is(err, ErrConfigNotFound) {
		t.Fatalf("expected ErrConfigNotFound, got %v", err)
	}
}

func TestResolveServer(t *testing.T) {
	cfg := &Config{
		DefaultServer: "DevServer",
		Servers: map[string]ServerEntry{
			"DevServer": {ConnectionString: "conn"},
			"Other":     {ConnectionString: "conn2"},
		},
	}

	name, _, err := cfg.ResolveServer("")
	if err != nil {
		t.Fatalf("resolve default error: %v", err)
	}
	if name != "DevServer" {
		t.Fatalf("expected DevServer, got %q", name)
	}

	name, _, err = cfg.ResolveServer("Other")
	if err != nil {
		t.Fatalf("resolve explicit error: %v", err)
	}
	if name != "Other" {
		t.Fatalf("expected Other, got %q", name)
	}

	_, _, err = cfg.ResolveServer("Missing")
	if !errors.Is(err, ErrServerNotFound) {
		t.Fatalf("expected ErrServerNotFound, got %v", err)
	}
}

func TestResolveServerNoDefault(t *testing.T) {
	cfg := &Config{Servers: map[string]ServerEntry{}}
	_, _, err := cfg.ResolveServer("")
	if !errors.Is(err, ErrNoServer) {
		t.Fatalf("expected ErrNoServer, got %v", err)
	}
}

func TestSaveToPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	cfg := &Config{
		DefaultServer: "DevServer",
		Servers: map[string]ServerEntry{
			"DevServer": {ConnectionString: "Host=localhost;Database=pgmq;"},
		},
	}

	if err := SaveToPath(path, cfg); err != nil {
		t.Fatalf("SaveToPath error: %v", err)
	}

	loaded, err := LoadFromPath(path)
	if err != nil {
		t.Fatalf("LoadFromPath error: %v", err)
	}
	if loaded.DefaultServer != "DevServer" {
		t.Fatalf("expected defaultServer DevServer, got %q", loaded.DefaultServer)
	}
	if loaded.Servers["DevServer"].ConnectionString == "" {
		t.Fatalf("expected connection string")
	}
}
