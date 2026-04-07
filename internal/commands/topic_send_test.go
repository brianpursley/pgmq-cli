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
	"reflect"
	"strings"
	"testing"
	"time"

	"pgmq-cli/internal/errs"
)

func TestTopicSendFlagValidation(t *testing.T) {
	cmd := TopicSendCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	cmd.PersistentFlags().String("config", "", "Config file path")
	_ = cmd.Flags().Set("delay", "5")
	_ = cmd.Flags().Set("delay-until", "2025-01-01T12:00:00Z")
	err := runTopicSend(cmd, "logs.api.error", `{"a":1}`)
	assertExitCode(t, err, errs.ExitUsage)

	cmd = TopicSendCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	cmd.PersistentFlags().String("config", "", "Config file path")
	_ = cmd.Flags().Set("delay-until", "bad-time")
	err = runTopicSend(cmd, "logs.api.error", `{"a":1}`)
	assertExitCode(t, err, errs.ExitUsage)

	cmd = TopicSendCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	cmd.PersistentFlags().String("config", "", "Config file path")
	err = runTopicSend(cmd, "logs.api.error", `not-json`)
	assertExitCode(t, err, errs.ExitUsage)

	cmd = TopicSendCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	cmd.PersistentFlags().String("config", "", "Config file path")
	_ = cmd.Flags().Set("headers", `not-json`)
	err = runTopicSend(cmd, "logs.api.error", `{"a":1}`)
	assertExitCode(t, err, errs.ExitUsage)

	cmd = TopicSendCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	cmd.PersistentFlags().String("config", "", "Config file path")
	_ = cmd.Flags().Set("delay", "-1")
	err = runTopicSend(cmd, "logs.api.error", `{"a":1}`)
	assertExitCode(t, err, errs.ExitUsage)
}

func TestBuildTopicSendBatchQuery(t *testing.T) {
	msg := json.RawMessage(`{"hello":"world"}`)
	headers := json.RawMessage(`{"priority":"high"}`)
	delayAt := time.Date(2026, 4, 7, 13, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		opts     topicSendOptions
		wantSQL  string
		wantArgs []any
	}{
		{
			name:     "no headers no delay",
			opts:     topicSendOptions{Message: msg},
			wantSQL:  "SELECT queue_name, msg_id FROM pgmq.send_batch_topic($1::text, ARRAY[$2::jsonb]);",
			wantArgs: []any{"logs.api.error", msg},
		},
		{
			name:     "headers no delay",
			opts:     topicSendOptions{Message: msg, Headers: headers, HasHeaders: true},
			wantSQL:  "SELECT queue_name, msg_id FROM pgmq.send_batch_topic($1::text, ARRAY[$2::jsonb], ARRAY[$3::jsonb]);",
			wantArgs: []any{"logs.api.error", msg, headers},
		},
		{
			name:     "no headers integer delay",
			opts:     topicSendOptions{Message: msg, DelaySeconds: 30},
			wantSQL:  "SELECT queue_name, msg_id FROM pgmq.send_batch_topic($1::text, ARRAY[$2::jsonb], $3::integer);",
			wantArgs: []any{"logs.api.error", msg, 30},
		},
		{
			name:     "headers integer delay",
			opts:     topicSendOptions{Message: msg, Headers: headers, HasHeaders: true, DelaySeconds: 30},
			wantSQL:  "SELECT queue_name, msg_id FROM pgmq.send_batch_topic($1::text, ARRAY[$2::jsonb], ARRAY[$3::jsonb], $4::integer);",
			wantArgs: []any{"logs.api.error", msg, headers, 30},
		},
		{
			name:     "no headers timestamp delay",
			opts:     topicSendOptions{Message: msg, DelayAt: delayAt, HasDelayAt: true},
			wantSQL:  "SELECT queue_name, msg_id FROM pgmq.send_batch_topic($1::text, ARRAY[$2::jsonb], $3::timestamptz);",
			wantArgs: []any{"logs.api.error", msg, delayAt},
		},
		{
			name:     "headers timestamp delay",
			opts:     topicSendOptions{Message: msg, Headers: headers, HasHeaders: true, DelayAt: delayAt, HasDelayAt: true},
			wantSQL:  "SELECT queue_name, msg_id FROM pgmq.send_batch_topic($1::text, ARRAY[$2::jsonb], ARRAY[$3::jsonb], $4::timestamptz);",
			wantArgs: []any{"logs.api.error", msg, headers, delayAt},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSQL, gotArgs := buildTopicSendBatchQuery("logs.api.error", tt.opts)
			if gotSQL != tt.wantSQL {
				t.Fatalf("unexpected sql:\n got: %s\nwant: %s", gotSQL, tt.wantSQL)
			}
			if !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Fatalf("unexpected args: got %#v want %#v", gotArgs, tt.wantArgs)
			}
		})
	}
}

func TestRenderTopicSendOutputTable(t *testing.T) {
	cmd := TopicSendCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	_ = cmd.Flags().Set("output", "table")

	records := []topicSendRecord{
		{QueueName: "all_logs", MsgID: 101},
		{QueueName: "error_logs", MsgID: 202},
	}

	if err := renderTopicSendOutput(cmd, records); err != nil {
		t.Fatalf("renderTopicSendOutput error: %v", err)
	}

	out := cmd.OutOrStdout().(*bytes.Buffer).String()
	for _, expected := range []string{"queue_name", "msg_id", "all_logs", "error_logs", "101", "202"} {
		if !strings.Contains(out, expected) {
			t.Fatalf("expected table output to contain %q, got %q", expected, out)
		}
	}
}

func TestRenderTopicSendOutputJSON(t *testing.T) {
	cmd := TopicSendCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	_ = cmd.Flags().Set("output", "json")

	records := []topicSendRecord{{QueueName: "all_logs", MsgID: 101}}
	if err := renderTopicSendOutput(cmd, records); err != nil {
		t.Fatalf("renderTopicSendOutput json error: %v", err)
	}

	var got []topicSendRecord
	if err := json.Unmarshal(cmd.OutOrStdout().(*bytes.Buffer).Bytes(), &got); err != nil {
		t.Fatalf("expected json array, got %v", err)
	}
	if !reflect.DeepEqual(got, records) {
		t.Fatalf("unexpected json payload: got %#v want %#v", got, records)
	}
}

func TestRenderTopicSendOutputEmptyJSON(t *testing.T) {
	cmd := TopicSendCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	_ = cmd.Flags().Set("output", "json")

	if err := renderTopicSendOutput(cmd, []topicSendRecord{}); err != nil {
		t.Fatalf("renderTopicSendOutput empty json error: %v", err)
	}

	var arr []any
	if err := json.Unmarshal(cmd.OutOrStdout().(*bytes.Buffer).Bytes(), &arr); err != nil {
		t.Fatalf("expected empty json array, got %v", err)
	}
	if len(arr) != 0 {
		t.Fatalf("expected empty array, got %#v", arr)
	}
}

func TestRenderTopicSendOutputEmptyTable(t *testing.T) {
	cmd := TopicSendCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	_ = cmd.Flags().Set("output", "table")

	if err := renderTopicSendOutput(cmd, []topicSendRecord{}); err != nil {
		t.Fatalf("renderTopicSendOutput empty table error: %v", err)
	}

	out := cmd.OutOrStdout().(*bytes.Buffer).String()
	if !strings.Contains(out, "no matching topic bindings") {
		t.Fatalf("expected empty table message, got %q", out)
	}
}
