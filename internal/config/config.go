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
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
)

// Config represents ~/.pgmq/config.json
// This is a minimal placeholder; fields will be expanded as implementation proceeds.
type Config struct {
	DefaultServer string                 `json:"defaultServer"`
	Servers       map[string]ServerEntry `json:"servers"`
}

type ServerEntry struct {
	ConnectionString string `json:"connectionString"`
}

var ErrServerNotFound = errors.New("server not found")
var ErrNoServer = errors.New("server name required")
var ErrConfigNotFound = errors.New("config not found")

// DefaultPath returns the default config path.
func DefaultPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".pgmq", "config.json"), nil
}

// LoadDefault loads the config from the default location.
func LoadDefault() (*Config, error) {
	path, err := DefaultPath()
	if err != nil {
		return nil, err
	}
	return LoadFromPath(path)
}

// LoadFromPath loads config from the provided path.
func LoadFromPath(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrConfigNotFound
		}
		return nil, err
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	if cfg.Servers == nil {
		cfg.Servers = map[string]ServerEntry{}
	}
	return &cfg, nil
}

// SaveToPath writes the config to the provided path.
func SaveToPath(path string, cfg *Config) error {
	if cfg == nil {
		return errors.New("config is nil")
	}
	if cfg.Servers == nil {
		cfg.Servers = map[string]ServerEntry{}
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o600)
}

// LoadOrInit loads config or returns an empty config if it does not exist.
func LoadOrInit(path string) (*Config, error) {
	cfg, err := LoadFromPath(path)
	if err != nil {
		if errors.Is(err, ErrConfigNotFound) {
			return &Config{Servers: map[string]ServerEntry{}}, nil
		}
		return nil, err
	}
	if cfg.Servers == nil {
		cfg.Servers = map[string]ServerEntry{}
	}
	return cfg, nil
}

// ResolveServer selects a server by name or default.
func (c *Config) ResolveServer(name string) (string, ServerEntry, error) {
	if name == "" {
		name = c.DefaultServer
	}
	if name == "" {
		return "", ServerEntry{}, ErrNoServer
	}
	entry, ok := c.Servers[name]
	if !ok {
		return "", ServerEntry{}, ErrServerNotFound
	}
	return name, entry, nil
}

// ServerNames returns a sorted list of server names.
func (c *Config) ServerNames() []string {
	names := make([]string, 0, len(c.Servers))
	for name := range c.Servers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
