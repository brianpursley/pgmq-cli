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
	"reflect"
	"strings"
	"testing"

	"pgmq-cli/internal/errs"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/spf13/cobra"
)

func TestReadNumericValidation(t *testing.T) {
	cmd := ReadCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	cmd.PersistentFlags().String("config", "", "Config file path")
	cmd.Flags().Set("qty", "0")
	err := runRead(cmd, "queue")
	assertExitCode(t, err, errs.ExitUsage)

	cmd = ReadCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	cmd.PersistentFlags().String("config", "", "Config file path")
	cmd.Flags().Set("vt", "-1")
	err = runRead(cmd, "queue")
	assertExitCode(t, err, errs.ExitUsage)
}

func TestReadStrategyValidation(t *testing.T) {
	cmd := ReadCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	cmd.PersistentFlags().String("config", "", "Config file path")
	cmd.Flags().Set("strategy", "bad")
	err := runRead(cmd, "queue")
	assertExitCode(t, err, errs.ExitUsage)
}

func TestReadQueryForStrategy(t *testing.T) {
	tests := []struct {
		strategy string
		want     string
	}{
		{
			strategy: readStrategyStandard,
			want:     "SELECT msg_id, read_ct, enqueued_at, vt, message, headers FROM pgmq.read($1::text, $2, $3);",
		},
		{
			strategy: readStrategyGrouped,
			want:     "SELECT msg_id, read_ct, enqueued_at, vt, message, headers FROM pgmq.read_grouped($1::text, $2, $3);",
		},
		{
			strategy: readStrategyGroupedRR,
			want:     "SELECT msg_id, read_ct, enqueued_at, vt, message, headers FROM pgmq.read_grouped_rr($1::text, $2, $3);",
		},
		{
			strategy: readStrategyGroupedHead,
			want:     "SELECT msg_id, read_ct, enqueued_at, vt, message, headers FROM pgmq.read_grouped_head($1::text, $2, $3);",
		},
	}

	for _, tt := range tests {
		got, err := readQueryForStrategy(tt.strategy)
		if err != nil {
			t.Fatalf("readQueryForStrategy(%q) unexpected error: %v", tt.strategy, err)
		}
		if got != tt.want {
			t.Fatalf("readQueryForStrategy(%q) = %q, want %q", tt.strategy, got, tt.want)
		}
	}
}

func TestReadStrategyCompletion(t *testing.T) {
	got, directive := readStrategyCompletion(&cobra.Command{Use: "read"}, nil, "")
	if !reflect.DeepEqual(got, readStrategies) {
		t.Fatalf("unexpected completions: got %v want %v", got, readStrategies)
	}
	if directive != cobra.ShellCompDirectiveNoFileComp {
		t.Fatalf("unexpected directive: %v", directive)
	}

	got, _ = readStrategyCompletion(&cobra.Command{Use: "read"}, nil, "grouped-r")
	want := []string{readStrategyGroupedRR}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected filtered completions: got %v want %v", got, want)
	}
}

func TestDBErrorForReadStrategy(t *testing.T) {
	err := dbErrorForReadStrategy(&pgconn.PgError{Code: "42883", Message: "function does not exist"}, "queue", readStrategyStandard)
	assertExitCode(t, err, errs.ExitError)
	if !strings.Contains(err.Error(), "pgmq extension init") {
		t.Fatalf("expected standard read extension init message, got %q", err.Error())
	}

	err = dbErrorForReadStrategy(&pgconn.PgError{Code: "42883", Message: "function does not exist"}, "queue", readStrategyGrouped)
	assertExitCode(t, err, errs.ExitError)
	if !strings.Contains(err.Error(), "FIFO functions not found") {
		t.Fatalf("expected FIFO read message, got %q", err.Error())
	}
}
