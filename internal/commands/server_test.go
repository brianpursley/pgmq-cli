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

package commands

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/spf13/cobra"

	"pgmq-cli/internal/config"
	"pgmq-cli/internal/errs"
)

func TestServerCommandsWithConfigFlag(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")

	root := newTestRoot()
	root.SetArgs([]string{"server", "add", "DevServer", "Host=localhost;Database=pgmq;", "--config", cfgPath})
	out, err := execute(root)
	if err != nil {
		t.Fatalf("server add error: %v", err)
	}
	if !bytes.Contains(out, []byte("server \"DevServer\" added")) {
		t.Fatalf("unexpected output: %s", string(out))
	}

	root = newTestRoot()
	root.SetArgs([]string{"server", "set-default", "DevServer", "--config", cfgPath})
	_, err = execute(root)
	if err != nil {
		t.Fatalf("set-default error: %v", err)
	}

	root = newTestRoot()
	root.SetArgs([]string{"server", "get", "DevServer", "--config", cfgPath, "-o", "json"})
	out, err = execute(root)
	if err != nil {
		t.Fatalf("server get error: %v", err)
	}
	if !bytes.Contains(out, []byte("\"name\": \"DevServer\"")) {
		t.Fatalf("unexpected get output: %s", string(out))
	}

	root = newTestRoot()
	root.SetArgs([]string{"server", "get-default", "--config", cfgPath, "-o", "json"})
	out, err = execute(root)
	if err != nil {
		t.Fatalf("get-default error: %v", err)
	}
	if !bytes.Contains(out, []byte("\"defaultServer\": \"DevServer\"")) {
		t.Fatalf("unexpected get-default output: %s", string(out))
	}

	root = newTestRoot()
	root.SetArgs([]string{"server", "list", "--config", cfgPath, "-o", "json"})
	out, err = execute(root)
	if err != nil {
		t.Fatalf("server list error: %v", err)
	}
	if !bytes.Contains(out, []byte("DevServer")) {
		t.Fatalf("unexpected list output: %s", string(out))
	}
	if bytes.Contains(out, []byte("connectionString")) || bytes.Contains(out, []byte("pgmq_test")) || bytes.Contains(out, []byte("pgmq;")) {
		t.Fatalf("list output should not contain connection strings: %s", string(out))
	}

	root = newTestRoot()
	root.SetArgs([]string{"server", "update", "DevServer", "Host=localhost;Database=pgmq_test;", "--config", cfgPath})
	_, err = execute(root)
	if err != nil {
		t.Fatalf("server update error: %v", err)
	}

	root = newTestRoot()
	root.SetArgs([]string{"server", "unset-default", "--config", cfgPath})
	_, err = execute(root)
	if err != nil {
		t.Fatalf("unset-default error: %v", err)
	}

	root = newTestRoot()
	root.SetArgs([]string{"server", "remove", "DevServer", "--config", cfgPath})
	_, err = execute(root)
	if err != nil {
		t.Fatalf("server remove error: %v", err)
	}

	if _, err := os.Stat(cfgPath); err != nil {
		t.Fatalf("expected config file to exist: %v", err)
	}
}

func newTestRoot() *cobra.Command {
	root := &cobra.Command{Use: "pgmq"}
	root.SetOut(&bytes.Buffer{})
	root.SetErr(&bytes.Buffer{})
	root.SetIn(&bytes.Buffer{})

	root.PersistentFlags().StringP("server", "s", "", "Server name")
	root.PersistentFlags().BoolP("yes", "Y", false, "Automatic yes to prompts")
	root.PersistentFlags().String("config", "", "Config file path")

	root.AddCommand(ServerCmd())
	return root
}

func execute(cmd *cobra.Command) ([]byte, error) {
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(&bytes.Buffer{})
	err := cmd.Execute()
	return out.Bytes(), err
}

func TestServerCommandErrorCodes(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "missing.json")

	root := newTestRoot()
	root.SetArgs([]string{"server", "get", "DevServer", "--config", cfgPath})
	_, err := execute(root)
	assertExitCode(t, err, errs.ExitUsage)

	root = newTestRoot()
	root.SetArgs([]string{"server", "add", "DevServer", "Host=localhost;Database=pgmq;", "--config", cfgPath})
	_, err = execute(root)
	if err != nil {
		t.Fatalf("server add error: %v", err)
	}

	root = newTestRoot()
	root.SetArgs([]string{"server", "update", "Missing", "Host=localhost;Database=pgmq;", "--config", cfgPath})
	_, err = execute(root)
	assertExitCode(t, err, errs.ExitNotFound)

	root = newTestRoot()
	root.SetArgs([]string{"server", "get-default", "--config", cfgPath})
	_, err = execute(root)
	assertExitCode(t, err, errs.ExitNotFound)
}

func TestServerNameCompletion(t *testing.T) {
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

	cmd := &cobra.Command{Use: "get"}
	cmd.Flags().String("config", "", "Config file path")
	_ = cmd.Flags().Set("config", cfgPath)

	got, directive := serverNameCompletion(cmd, nil, "")
	want := []string{"DevServer", "Prod"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected completions: got %v want %v", got, want)
	}
	if directive != cobra.ShellCompDirectiveNoFileComp {
		t.Fatalf("unexpected directive: %v", directive)
	}

	got, _ = serverNameCompletion(cmd, nil, "De")
	want = []string{"DevServer"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected filtered completions: got %v want %v", got, want)
	}

	got, _ = serverNameCompletion(cmd, []string{"DevServer"}, "")
	if len(got) != 0 {
		t.Fatalf("expected no completions after first arg, got %v", got)
	}
}

func TestServerCommandsWireNameCompletion(t *testing.T) {
	if ServerGetCmd().ValidArgsFunction == nil {
		t.Fatalf("server get should have name completion")
	}
	if ServerRemoveCmd().ValidArgsFunction == nil {
		t.Fatalf("server remove should have name completion")
	}
	if ServerSetDefaultCmd().ValidArgsFunction == nil {
		t.Fatalf("server set-default should have name completion")
	}
	if ServerUpdateCmd().ValidArgsFunction == nil {
		t.Fatalf("server update should have name completion")
	}
}
