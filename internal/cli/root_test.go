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

package cli

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/spf13/cobra"

	"pgmq-cli/internal/config"
)

func TestServerFlagCompletion(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	cfg := &config.Config{
		DefaultServer: "DevServer",
		Servers: map[string]config.ServerEntry{
			"DevServer": {ConnectionString: "Host=localhost;Database=pgmq;"},
			"Prod":      {ConnectionString: "Host=localhost;Database=pgmq_prod;"},
		},
	}
	if err := config.SaveToPath(cfgPath, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	cmd := &cobra.Command{Use: "pgmq"}
	cmd.Flags().String("config", "", "Config file path")
	_ = cmd.Flags().Set("config", cfgPath)

	got, directive := serverFlagCompletion(cmd, nil, "")
	want := []string{"DevServer", "Prod"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected completions: got %v want %v", got, want)
	}
	if directive != cobra.ShellCompDirectiveNoFileComp {
		t.Fatalf("unexpected directive: %v", directive)
	}

	got, _ = serverFlagCompletion(cmd, nil, "De")
	want = []string{"DevServer"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected filtered completions: got %v want %v", got, want)
	}
}

func TestServerFlagCompletionMissingConfig(t *testing.T) {
	cmd := &cobra.Command{Use: "pgmq"}
	cmd.Flags().String("config", "", "Config file path")
	_ = cmd.Flags().Set("config", filepath.Join(t.TempDir(), "missing.json"))

	got, directive := serverFlagCompletion(cmd, nil, "")
	if len(got) != 0 {
		t.Fatalf("expected no completions for missing config, got %v", got)
	}
	if directive != cobra.ShellCompDirectiveNoFileComp {
		t.Fatalf("unexpected directive: %v", directive)
	}
}

func TestConfigFlagCompletion(t *testing.T) {
	got, directive := configFlagCompletion(&cobra.Command{Use: "pgmq"}, nil, "")
	want := []string{"json"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected completions: got %v want %v", got, want)
	}
	if directive != cobra.ShellCompDirectiveFilterFileExt {
		t.Fatalf("unexpected directive: %v", directive)
	}
}

func TestRootIncludesVersionCommand(t *testing.T) {
	root := NewRootCmd()
	if _, _, err := root.Find([]string{"version"}); err != nil {
		t.Fatalf("expected version command to be registered: %v", err)
	}
}
