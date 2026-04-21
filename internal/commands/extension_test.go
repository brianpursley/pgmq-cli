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
	"encoding/json"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"pgmq-cli/internal/errs"
)

func TestExtensionSubcommandsRegisterExpectedChildren(t *testing.T) {
	cmd := ExtensionCmd()

	for _, name := range []string{"init", "status", "version"} {
		if _, _, err := cmd.Find([]string{name}); err != nil {
			t.Fatalf("expected extension subcommand %q to be registered: %v", name, err)
		}
	}
}

func TestExtensionCheckIsNotRegistered(t *testing.T) {
	cmd := ExtensionCmd()
	if found, _, err := cmd.Find([]string{"check"}); err == nil && found.Name() == "check" {
		t.Fatalf("expected extension check not to be registered")
	}
}

func TestExtensionInitDoesNotExposeCheckFlag(t *testing.T) {
	cmd := ExtensionInitCmd()
	if cmd.Flags().Lookup("check") != nil {
		t.Fatalf("expected extension init not to expose --check")
	}
}

func TestInitCmdHidden(t *testing.T) {
	cmd := InitCmd()
	if !cmd.Hidden {
		t.Fatalf("expected legacy init command to be hidden")
	}
}

func TestInitCmdDeprecationWarningWithoutConfig(t *testing.T) {
	cmd := InitCmd()
	cmd.SetOut(&bytes.Buffer{})
	errOut := &bytes.Buffer{}
	cmd.SetErr(errOut)
	cmd.SetIn(&bytes.Buffer{})
	cmd.PersistentFlags().String("config", "", "Config file path")

	err := cmd.Execute()
	assertExitCode(t, err, errs.ExitUsage)
	if !strings.Contains(errOut.String(), "pgmq init is deprecated; use pgmq extension init instead") {
		t.Fatalf("expected init deprecation warning on stderr, got %q", errOut.String())
	}
}

func TestInitCheckDeprecationWarningWithoutConfig(t *testing.T) {
	cmd := InitCmd()
	cmd.SetOut(&bytes.Buffer{})
	errOut := &bytes.Buffer{}
	cmd.SetErr(errOut)
	cmd.SetIn(&bytes.Buffer{})
	cmd.PersistentFlags().String("config", "", "Config file path")
	_ = cmd.Flags().Set("check", "true")

	err := cmd.Execute()
	assertExitCode(t, err, errs.ExitUsage)
	if !strings.Contains(errOut.String(), "pgmq init --check is deprecated; use pgmq extension status instead") {
		t.Fatalf("expected init --check deprecation warning on stderr, got %q", errOut.String())
	}
}

func TestRenderExtensionStatus(t *testing.T) {
	version := "1.11.1"
	record := extensionStatusRecord{Initialized: true, Version: &version}

	cmd := &cobra.Command{Use: "status"}
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	addOutputFlag(cmd, false)

	if err := renderExtensionStatus(cmd, record); err != nil {
		t.Fatalf("renderExtensionStatus table error: %v", err)
	}
	out := cmd.OutOrStdout().(*bytes.Buffer).String()
	for _, expected := range []string{"initialized", "version", "true", version} {
		if !strings.Contains(out, expected) {
			t.Fatalf("expected status output to contain %q, got %q", expected, out)
		}
	}

	cmd.SetOut(&bytes.Buffer{})
	_ = cmd.Flags().Set("output", "json")
	if err := renderExtensionStatus(cmd, record); err != nil {
		t.Fatalf("renderExtensionStatus json error: %v", err)
	}
	var got extensionStatusRecord
	if err := json.Unmarshal(cmd.OutOrStdout().(*bytes.Buffer).Bytes(), &got); err != nil {
		t.Fatalf("expected status json object: %v", err)
	}
	if !got.Initialized || got.Version == nil || *got.Version != version {
		t.Fatalf("unexpected status json: %#v", got)
	}
}

func TestRenderExtensionStatusNotInitialized(t *testing.T) {
	cmd := &cobra.Command{Use: "status"}
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	addOutputFlag(cmd, false)
	_ = cmd.Flags().Set("output", "json")

	if err := renderExtensionStatus(cmd, extensionStatusRecord{}); err != nil {
		t.Fatalf("renderExtensionStatus not initialized json error: %v", err)
	}
	var got extensionStatusRecord
	if err := json.Unmarshal(cmd.OutOrStdout().(*bytes.Buffer).Bytes(), &got); err != nil {
		t.Fatalf("expected status json object: %v", err)
	}
	if got.Initialized || got.Version != nil {
		t.Fatalf("expected not initialized status, got %#v", got)
	}
}

func TestRenderExtensionVersion(t *testing.T) {
	cmd := &cobra.Command{Use: "version"}
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	addOutputFlag(cmd, false)

	if err := renderExtensionVersion(cmd, "1.11.1"); err != nil {
		t.Fatalf("renderExtensionVersion table error: %v", err)
	}
	if strings.TrimSpace(cmd.OutOrStdout().(*bytes.Buffer).String()) != "1.11.1" {
		t.Fatalf("unexpected version output: %q", cmd.OutOrStdout().(*bytes.Buffer).String())
	}

	cmd.SetOut(&bytes.Buffer{})
	_ = cmd.Flags().Set("output", "json")
	if err := renderExtensionVersion(cmd, "1.11.1"); err != nil {
		t.Fatalf("renderExtensionVersion json error: %v", err)
	}
	var version string
	if err := json.Unmarshal(cmd.OutOrStdout().(*bytes.Buffer).Bytes(), &version); err != nil {
		t.Fatalf("expected json string output: %v", err)
	}
	if version != "1.11.1" {
		t.Fatalf("unexpected json version: %q", version)
	}
}
