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
	"time"

	"github.com/spf13/cobra"

	"pgmq-cli/internal/errs"
)

func TestTopicSubcommandsRegisterExpectedChildren(t *testing.T) {
	cmd := TopicCmd()

	for _, name := range []string{"bind", "unbind", "list", "test", "send"} {
		if _, _, err := cmd.Find([]string{name}); err != nil {
			t.Fatalf("expected topic subcommand %q to be registered: %v", name, err)
		}
	}
}

func TestTopicCommandsWireQueueCompletion(t *testing.T) {
	if TopicBindCmd().ValidArgsFunction == nil {
		t.Fatalf("expected topic bind to wire queue completion")
	}
	if TopicUnbindCmd().ValidArgsFunction == nil {
		t.Fatalf("expected topic unbind to wire queue completion")
	}
	if TopicListCmd().ValidArgsFunction == nil {
		t.Fatalf("expected topic list to wire queue completion")
	}
}

func TestTopicQueueSecondArgCompletionOnlyOnSecondArg(t *testing.T) {
	cmd := &cobra.Command{Use: "bind"}

	completions, directive := topicQueueSecondArgCompletion(cmd, nil, "")
	if len(completions) != 0 {
		t.Fatalf("expected no completions before second arg, got %v", completions)
	}
	if directive != cobra.ShellCompDirectiveNoFileComp {
		t.Fatalf("unexpected directive: %v", directive)
	}

	completions, directive = topicQueueSecondArgCompletion(cmd, []string{"pattern"}, "")
	if len(completions) != 0 {
		t.Fatalf("expected graceful empty completions without config, got %v", completions)
	}
	if directive != cobra.ShellCompDirectiveNoFileComp {
		t.Fatalf("unexpected directive: %v", directive)
	}
}

func TestTopicBindingNotFoundError(t *testing.T) {
	err := topicBindingNotFoundError("logs.#", "audit")
	assertExitCode(t, err, errs.ExitNotFound)
	if !strings.Contains(err.Error(), `pattern "logs.#"`) {
		t.Fatalf("expected pattern in error, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), `queue "audit"`) {
		t.Fatalf("expected queue in error, got %q", err.Error())
	}
}

func TestRenderTopicBindingOutputTable(t *testing.T) {
	cmd := &cobra.Command{Use: "topic list"}
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	cmd.Flags().StringP("output", "o", "table", "Output format")
	_ = cmd.Flags().Set("output", "table")

	records := []topicBindingRecord{{
		Pattern:       "logs.#",
		QueueName:     "all_logs",
		BoundAt:       time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC),
		CompiledRegex: `^logs\..*$`,
	}}

	if err := renderTopicBindingOutput(cmd, records); err != nil {
		t.Fatalf("renderTopicBindingOutput error: %v", err)
	}

	out := cmd.OutOrStdout().(*bytes.Buffer).String()
	for _, expected := range []string{"pattern", "queue_name", "bound_at", "compiled_regex", "logs.#", "all_logs", "2026-04-07T12:00:00Z", `^logs\..*$`} {
		if !strings.Contains(out, expected) {
			t.Fatalf("expected table output to contain %q, got %q", expected, out)
		}
	}
}

func TestRenderTopicBindingOutputEmptyJSON(t *testing.T) {
	cmd := &cobra.Command{Use: "topic list"}
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	cmd.Flags().StringP("output", "o", "table", "Output format")
	_ = cmd.Flags().Set("output", "json")

	if err := renderTopicBindingOutput(cmd, []topicBindingRecord{}); err != nil {
		t.Fatalf("renderTopicBindingOutput empty json error: %v", err)
	}

	var arr []any
	if err := json.Unmarshal(cmd.OutOrStdout().(*bytes.Buffer).Bytes(), &arr); err != nil {
		t.Fatalf("expected empty json array, got %v", err)
	}
	if len(arr) != 0 {
		t.Fatalf("expected empty array, got %#v", arr)
	}
}

func TestRenderTopicRouteOutputTable(t *testing.T) {
	cmd := &cobra.Command{Use: "topic test"}
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	cmd.Flags().StringP("output", "o", "table", "Output format")
	_ = cmd.Flags().Set("output", "table")

	records := []topicRouteRecord{{
		Pattern:       "logs.*.error",
		QueueName:     "error_logs",
		CompiledRegex: `^logs\.[^.]+\.error$`,
	}}

	if err := renderTopicRouteOutput(cmd, records); err != nil {
		t.Fatalf("renderTopicRouteOutput error: %v", err)
	}

	out := cmd.OutOrStdout().(*bytes.Buffer).String()
	for _, expected := range []string{"pattern", "queue_name", "compiled_regex", "logs.*.error", "error_logs", `^logs\.[^.]+\.error$`} {
		if !strings.Contains(out, expected) {
			t.Fatalf("expected table output to contain %q, got %q", expected, out)
		}
	}
}

func TestRenderTopicRouteOutputEmptyJSON(t *testing.T) {
	cmd := &cobra.Command{Use: "topic test"}
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	cmd.Flags().StringP("output", "o", "table", "Output format")
	_ = cmd.Flags().Set("output", "json")

	if err := renderTopicRouteOutput(cmd, []topicRouteRecord{}); err != nil {
		t.Fatalf("renderTopicRouteOutput empty json error: %v", err)
	}

	var arr []any
	if err := json.Unmarshal(cmd.OutOrStdout().(*bytes.Buffer).Bytes(), &arr); err != nil {
		t.Fatalf("expected empty json array, got %v", err)
	}
	if len(arr) != 0 {
		t.Fatalf("expected empty array, got %#v", arr)
	}
}
