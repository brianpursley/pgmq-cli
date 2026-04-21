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
	"pgmq-cli/internal/errs"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/spf13/cobra"
)

func TestOutputEmpty(t *testing.T) {
	cmd := &cobra.Command{Use: "pgmq"}
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	cmd.Flags().StringP("output", "o", "table", "Output format")
	_ = cmd.Flags().Set("output", "table")
	if err := outputEmpty(cmd, "no messages found"); err != nil {
		t.Fatalf("outputEmpty error: %v", err)
	}

	cmd.SetOut(&bytes.Buffer{})
	_ = cmd.Flags().Set("output", "json")
	if err := outputEmpty(cmd, "no messages found"); err != nil {
		t.Fatalf("outputEmpty json error: %v", err)
	}
	var arr []any
	if err := json.Unmarshal(cmd.OutOrStdout().(*bytes.Buffer).Bytes(), &arr); err != nil {
		t.Fatalf("expected json array output for empty json mode: %v", err)
	}
	if len(arr) != 0 {
		t.Fatalf("expected empty array, got %v", arr)
	}
}

func TestOutputJSONByQty(t *testing.T) {
	cmd := &cobra.Command{Use: "pgmq"}
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	cmd.Flags().StringP("output", "o", "table", "Output format")
	_ = cmd.Flags().Set("output", "json")

	rec := readRecord{MsgID: 1}
	if err := outputJSONByQty(cmd, 1, []readRecord{rec}); err != nil {
		t.Fatalf("outputJSONByQty error: %v", err)
	}
	var obj map[string]any
	if err := json.Unmarshal(cmd.OutOrStdout().(*bytes.Buffer).Bytes(), &obj); err != nil {
		t.Fatalf("expected json object: %v", err)
	}

	cmd.SetOut(&bytes.Buffer{})
	if err := outputJSONByQty(cmd, 2, []readRecord{rec, rec}); err != nil {
		t.Fatalf("outputJSONByQty array error: %v", err)
	}
	var arr []any
	if err := json.Unmarshal(cmd.OutOrStdout().(*bytes.Buffer).Bytes(), &arr); err != nil {
		t.Fatalf("expected json array: %v", err)
	}
}

func TestGetOutputFormatValidation(t *testing.T) {
	cmd := &cobra.Command{Use: "read"}
	cmd.Flags().StringP("output", "o", "table", "Output format")
	_ = cmd.Flags().Set("output", "message")
	err := func() error {
		_, err := getOutputFormat(cmd)
		return err
	}()
	assertExitCode(t, err, errs.ExitUsage)

	cmd = &cobra.Command{Use: "pop"}
	cmd.Flags().StringP("output", "o", "table", "Output format")
	_ = cmd.Flags().Set("output", "message")
	if _, err := getOutputFormat(cmd); err != nil {
		t.Fatalf("expected message output for pop, got %v", err)
	}

	cmd = &cobra.Command{Use: "pop"}
	cmd.Flags().StringP("output", "o", "table", "Output format")
	_ = cmd.Flags().Set("output", "invalid")
	err = func() error {
		_, err := getOutputFormat(cmd)
		return err
	}()
	assertExitCode(t, err, errs.ExitUsage)
}

func TestDBErrorForQueue(t *testing.T) {
	err := dbErrorForQueue(&pgconn.PgError{Code: "42P01", Message: "relation does not exist"}, "q1")
	assertExitCode(t, err, errs.ExitNotFound)
	if !strings.Contains(err.Error(), "q1") {
		t.Fatalf("expected queue name in error, got %q", err.Error())
	}

	err = dbErrorForQueue(&pgconn.PgError{Code: "42883", Message: "function does not exist"}, "q1")
	assertExitCode(t, err, errs.ExitError)
}

func TestDBErrorForTopic(t *testing.T) {
	err := dbErrorForTopic(&pgconn.PgError{Code: "42883", Message: "function does not exist"})
	assertExitCode(t, err, errs.ExitError)
	if !strings.Contains(err.Error(), "topic routing functions not found") {
		t.Fatalf("expected topic routing message, got %q", err.Error())
	}
}

func TestDBErrorForTopicQueue(t *testing.T) {
	err := dbErrorForTopicQueue(&pgconn.PgError{Code: "42P01", Message: "relation does not exist"}, "q1")
	assertExitCode(t, err, errs.ExitNotFound)
	if !strings.Contains(err.Error(), "q1") {
		t.Fatalf("expected queue name in error, got %q", err.Error())
	}

	err = dbErrorForTopicQueue(&pgconn.PgError{Code: "42883", Message: "function does not exist"}, "q1")
	assertExitCode(t, err, errs.ExitError)
	if !strings.Contains(err.Error(), "topic routing functions not found") {
		t.Fatalf("expected topic routing message, got %q", err.Error())
	}
}

func TestDBErrorForFIFO(t *testing.T) {
	err := dbErrorForFIFO(&pgconn.PgError{Code: "42883", Message: "function does not exist"})
	assertExitCode(t, err, errs.ExitError)
	if !strings.Contains(err.Error(), "FIFO functions not found") {
		t.Fatalf("expected FIFO functions message, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "1.11.1 or later") {
		t.Fatalf("expected FIFO version message, got %q", err.Error())
	}
}

func TestDBErrorForFIFOQueue(t *testing.T) {
	err := dbErrorForFIFOQueue(&pgconn.PgError{Code: "42P01", Message: "relation does not exist"}, "q1")
	assertExitCode(t, err, errs.ExitNotFound)
	if !strings.Contains(err.Error(), "q1") {
		t.Fatalf("expected queue name in error, got %q", err.Error())
	}

	err = dbErrorForFIFOQueue(&pgconn.PgError{Code: "42883", Message: "function does not exist"}, "q1")
	assertExitCode(t, err, errs.ExitError)
	if !strings.Contains(err.Error(), "FIFO functions not found") {
		t.Fatalf("expected FIFO functions message, got %q", err.Error())
	}
}

func TestOutputJSONByQtyEmpty(t *testing.T) {
	cmd := &cobra.Command{Use: "pgmq"}
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	cmd.Flags().StringP("output", "o", "table", "Output format")
	_ = cmd.Flags().Set("output", "json")

	if err := outputJSONByQty(cmd, 1, []readRecord{}); err != nil {
		t.Fatalf("outputJSONByQty empty error: %v", err)
	}
	if cmd.OutOrStdout().(*bytes.Buffer).Len() != 0 {
		t.Fatalf("expected no output for empty json")
	}
}

func TestRenderOutputEmptyBehavior(t *testing.T) {
	cmd := &cobra.Command{Use: "list"}
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	cmd.Flags().StringP("output", "o", "table", "Output format")
	_ = cmd.Flags().Set("output", "table")

	err := renderOutput(cmd, []string{"a"}, [][]string{}, []map[string]any{}, "no rows")
	if err != nil {
		t.Fatalf("renderOutput table error: %v", err)
	}
	if !strings.Contains(cmd.OutOrStdout().(*bytes.Buffer).String(), "no rows") {
		t.Fatalf("expected empty message, got %q", cmd.OutOrStdout().(*bytes.Buffer).String())
	}

	cmd.SetOut(&bytes.Buffer{})
	_ = cmd.Flags().Set("output", "json")
	err = renderOutput(cmd, []string{"a"}, [][]string{}, []map[string]any{}, "no rows")
	if err != nil {
		t.Fatalf("renderOutput json error: %v", err)
	}
	var arr []any
	if err := json.Unmarshal(cmd.OutOrStdout().(*bytes.Buffer).Bytes(), &arr); err != nil {
		t.Fatalf("expected json array output for empty renderOutput: %v", err)
	}
	if len(arr) != 0 {
		t.Fatalf("expected empty array, got %v", arr)
	}
}

func TestOutputEmptyByQtyJSON(t *testing.T) {
	cmd := &cobra.Command{Use: "read"}
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	cmd.Flags().StringP("output", "o", "table", "Output format")
	_ = cmd.Flags().Set("output", "json")

	if err := outputEmptyByQty(cmd, 1, "no messages found"); err != nil {
		t.Fatalf("outputEmptyByQty qty=1 error: %v", err)
	}
	var single any
	if err := json.Unmarshal(cmd.OutOrStdout().(*bytes.Buffer).Bytes(), &single); err != nil {
		t.Fatalf("expected valid json for qty=1: %v", err)
	}
	if single != nil {
		t.Fatalf("expected null for qty=1, got %#v", single)
	}

	cmd.SetOut(&bytes.Buffer{})
	if err := outputEmptyByQty(cmd, 2, "no messages found"); err != nil {
		t.Fatalf("outputEmptyByQty qty=2 error: %v", err)
	}
	var arr []any
	if err := json.Unmarshal(cmd.OutOrStdout().(*bytes.Buffer).Bytes(), &arr); err != nil {
		t.Fatalf("expected valid json array for qty=2: %v", err)
	}
	if len(arr) != 0 {
		t.Fatalf("expected empty array for qty=2, got %#v", arr)
	}
}

func TestQueueNameCompletionGracefulFallback(t *testing.T) {
	cmd := &cobra.Command{Use: "read"}
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})

	completions, directive := queueNameCompletion(cmd, nil, "")
	if len(completions) != 0 {
		t.Fatalf("expected no completions without config, got %v", completions)
	}
	if directive != cobra.ShellCompDirectiveNoFileComp {
		t.Fatalf("expected no-file completion directive, got %v", directive)
	}
}

func TestQueueNameCompletionOnlyOnFirstArg(t *testing.T) {
	cmd := &cobra.Command{Use: "delete"}
	completions, directive := queueNameCompletion(cmd, []string{"queue"}, "")
	if len(completions) != 0 {
		t.Fatalf("expected no completions after first arg, got %v", completions)
	}
	if directive != cobra.ShellCompDirectiveNoFileComp {
		t.Fatalf("expected no-file completion directive, got %v", directive)
	}
}
