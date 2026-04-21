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
	"bytes"
	"path/filepath"
	"reflect"
	"strings"
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

func TestRootIncludesTopicCommand(t *testing.T) {
	root := NewRootCmd()
	if _, _, err := root.Find([]string{"topic"}); err != nil {
		t.Fatalf("expected topic command to be registered: %v", err)
	}
}

func TestRootIncludesFIFOCommand(t *testing.T) {
	root := NewRootCmd()
	if _, _, err := root.Find([]string{"fifo"}); err != nil {
		t.Fatalf("expected fifo command to be registered: %v", err)
	}
}

func TestRootIncludesExtensionCommand(t *testing.T) {
	root := NewRootCmd()
	if _, _, err := root.Find([]string{"extension"}); err != nil {
		t.Fatalf("expected extension command to be registered: %v", err)
	}
}

func TestRootInitCommandHidden(t *testing.T) {
	root := NewRootCmd()
	cmd, _, err := root.Find([]string{"init"})
	if err != nil {
		t.Fatalf("expected legacy init command to remain executable: %v", err)
	}
	if !cmd.Hidden {
		t.Fatalf("expected legacy init command to be hidden")
	}
}

func TestRootHelpHidesInitCommand(t *testing.T) {
	root := NewRootCmd()
	out := &bytes.Buffer{}
	errOut := &bytes.Buffer{}
	root.SetOut(out)
	root.SetErr(errOut)
	root.SetArgs([]string{"--help"})

	if err := root.Execute(); err != nil {
		t.Fatalf("root help failed: %v", err)
	}
	if strings.Contains(out.String(), "\n  init") {
		t.Fatalf("expected root help not to list hidden init command, got: %s", out.String())
	}
	if errOut.Len() != 0 {
		t.Fatalf("expected no stderr for root help, got %q", errOut.String())
	}
}

func TestRootInitHelpWorksWithoutDeprecationWarning(t *testing.T) {
	root := NewRootCmd()
	out := &bytes.Buffer{}
	errOut := &bytes.Buffer{}
	root.SetOut(out)
	root.SetErr(errOut)
	root.SetArgs([]string{"init", "--help"})

	if err := root.Execute(); err != nil {
		t.Fatalf("legacy init help failed: %v", err)
	}
	if !strings.Contains(out.String(), "Initialize pgmq extension") {
		t.Fatalf("expected init help output, got %q", out.String())
	}
	if strings.Contains(errOut.String(), "deprecated") {
		t.Fatalf("expected no deprecation warning for init help, got %q", errOut.String())
	}
}

func TestRootExtensionInitRejectsCheckFlag(t *testing.T) {
	root := NewRootCmd()
	root.SetOut(&bytes.Buffer{})
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"extension", "init", "--check"})

	err := root.Execute()
	if err == nil {
		t.Fatalf("expected extension init --check to fail")
	}
	if !strings.Contains(err.Error(), "unknown flag") {
		t.Fatalf("expected unknown flag error, got %v", err)
	}
}

func TestRootExtensionCheckNotRegistered(t *testing.T) {
	root := NewRootCmd()
	cmd, _, err := root.Find([]string{"extension", "check"})
	if err == nil && cmd.Name() == "check" {
		t.Fatalf("expected extension check not to be registered")
	}
}
