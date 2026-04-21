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
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"pgmq-cli/internal/errs"
)

func TestFIFOSubcommandsRegisterExpectedChildren(t *testing.T) {
	cmd := FIFOCmd()

	if _, _, err := cmd.Find([]string{"index"}); err != nil {
		t.Fatalf("expected fifo index subcommand to be registered: %v", err)
	}
}

func TestFIFOIndexArgsValidation(t *testing.T) {
	cmd := FIFOIndexCmd()
	if err := validateFIFOIndexArgs(cmd, []string{"queue"}); err != nil {
		t.Fatalf("expected queue argument to be valid, got %v", err)
	}

	cmd = FIFOIndexCmd()
	_ = cmd.Flags().Set("all", "true")
	if err := validateFIFOIndexArgs(cmd, nil); err != nil {
		t.Fatalf("expected --all to be valid, got %v", err)
	}

	cmd = FIFOIndexCmd()
	err := validateFIFOIndexArgs(cmd, nil)
	assertExitCode(t, err, errs.ExitUsage)
	if !strings.Contains(err.Error(), "queue required") {
		t.Fatalf("expected missing queue message, got %q", err.Error())
	}

	cmd = FIFOIndexCmd()
	err = validateFIFOIndexArgs(cmd, []string{"one", "two"})
	assertExitCode(t, err, errs.ExitUsage)
	if !strings.Contains(err.Error(), "at most one queue argument") {
		t.Fatalf("expected too many queue arguments message, got %q", err.Error())
	}

	cmd = FIFOIndexCmd()
	_ = cmd.Flags().Set("all", "true")
	err = validateFIFOIndexArgs(cmd, []string{"queue"})
	assertExitCode(t, err, errs.ExitUsage)
	if !strings.Contains(err.Error(), "--all cannot be used with a queue") {
		t.Fatalf("expected --all conflict message, got %q", err.Error())
	}
}

func TestFIFOIndexWiresQueueCompletion(t *testing.T) {
	if FIFOIndexCmd().ValidArgsFunction == nil {
		t.Fatalf("expected fifo index to wire queue completion")
	}
}

func TestFIFOIndexQueueCompletionDisabledByAll(t *testing.T) {
	cmd := FIFOIndexCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	_ = cmd.Flags().Set("all", "true")

	completions, directive := fifoIndexQueueCompletion(cmd, nil, "")
	if len(completions) != 0 {
		t.Fatalf("expected no completions with --all, got %v", completions)
	}
	if directive != cobra.ShellCompDirectiveNoFileComp {
		t.Fatalf("unexpected directive: %v", directive)
	}
}
