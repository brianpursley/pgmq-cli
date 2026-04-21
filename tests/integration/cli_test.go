//go:build integration

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

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type topicBindingResult struct {
	Pattern       string    `json:"pattern"`
	QueueName     string    `json:"queue_name"`
	BoundAt       time.Time `json:"bound_at"`
	CompiledRegex string    `json:"compiled_regex"`
}

type topicRouteResult struct {
	Pattern       string `json:"pattern"`
	QueueName     string `json:"queue_name"`
	CompiledRegex string `json:"compiled_regex"`
}

type topicSendResult struct {
	QueueName string `json:"queue_name"`
	MsgID     int64  `json:"msg_id"`
}

type queueReadResult struct {
	Message map[string]any `json:"message"`
	Headers map[string]any `json:"headers"`
}

func TestInitAndCreate(t *testing.T) {
	ctx := context.Background()

	container, host, port := startPGMQContainer(t, ctx)
	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	bin := buildBinary(t)
	home := setupHome(t, host, port)

	cfgPath := filepath.Join(home, ".pgmq", "config.json")

	runCmd := func(args ...string) (string, string, int) {
		cmd := exec.Command(bin, args...)
		cmd.Env = append(os.Environ(), "HOME="+home)
		var stdout, stderr strings.Builder
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err := cmd.Run()
		if err == nil {
			return stdout.String(), stderr.String(), 0
		}
		if exitErr, ok := err.(*exec.ExitError); ok {
			return stdout.String(), stderr.String(), exitErr.ExitCode()
		}
		t.Fatalf("command failed to run: %v", err)
		return "", "", -1
	}

	out, errOut, code := runCmd("init", "--config", cfgPath, "--server", "DevServer")
	if code != 0 {
		t.Fatalf("init failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if !strings.Contains(out, "extension initialized") && !strings.Contains(out, "extension already initialized") {
		t.Fatalf("unexpected init output: %q", out)
	}
	if !strings.Contains(strings.ToLower(out), "version") {
		t.Fatalf("expected init output to include extension version, got: %q", out)
	}

	out, errOut, code = runCmd("create", "--config", cfgPath, "--server", "DevServer", "test_queue")
	if code != 0 {
		t.Fatalf("create failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if !strings.Contains(out, "queue created") {
		t.Fatalf("unexpected create output: %q", out)
	}

	out, errOut, code = runCmd("metrics", "--config", cfgPath, "--server", "DevServer", "test_queue")
	if code != 0 {
		t.Fatalf("metrics on empty queue failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if !strings.Contains(out, "test_queue") {
		t.Fatalf("unexpected metrics output for empty queue: %q", out)
	}

	out, errOut, code = runCmd("metrics", "--config", cfgPath, "--server", "DevServer", "test_queue", "-o", "json")
	if code != 0 {
		t.Fatalf("metrics json on empty queue failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	var emptyMetrics map[string]any
	if err := json.Unmarshal([]byte(out), &emptyMetrics); err != nil {
		t.Fatalf("expected metrics json object, got %q", out)
	}
	if emptyMetrics["newest_msg_age_sec"] != nil {
		t.Fatalf("expected newest_msg_age_sec to be null, got %#v", emptyMetrics["newest_msg_age_sec"])
	}
	if emptyMetrics["oldest_msg_age_sec"] != nil {
		t.Fatalf("expected oldest_msg_age_sec to be null, got %#v", emptyMetrics["oldest_msg_age_sec"])
	}

	out, errOut, code = runCmd("list", "--config", cfgPath, "--server", "DevServer")
	if code != 0 {
		t.Fatalf("list-queues failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if !strings.Contains(out, "test_queue") {
		t.Fatalf("list-queues missing queue: %q", out)
	}

	out, errOut, code = runCmd("init", "--config", cfgPath, "--server", "DevServer", "--check")
	if code != 0 {
		t.Fatalf("init --check failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if !strings.Contains(out, "extension is initialized") {
		t.Fatalf("unexpected check output: %q", out)
	}
	if !strings.Contains(strings.ToLower(out), "version") {
		t.Fatalf("expected init --check output to include extension version, got: %q", out)
	}

	out, errOut, code = runCmd("send", "--config", cfgPath, "--server", "DevServer", "test_queue", `{"hello":"world"}`, "-o", "json")
	if code != 0 {
		t.Fatalf("send failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	var sendIDs []int64
	if err := json.Unmarshal([]byte(out), &sendIDs); err != nil || len(sendIDs) == 0 {
		t.Fatalf("expected send IDs, got %q", out)
	}

	out, errOut, code = runCmd("send", "--config", cfgPath, "--server", "DevServer", "test_queue", `{"hello":"delayed"}`, "--delay", "5", "-o", "json")
	if code != 0 {
		t.Fatalf("send with delay failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	var delayedIDs []int64
	if err := json.Unmarshal([]byte(out), &delayedIDs); err != nil || len(delayedIDs) == 0 {
		t.Fatalf("expected delayed send IDs, got %q", out)
	}

	delayUntil := time.Now().Add(5 * time.Minute).UTC().Format(time.RFC3339)
	out, errOut, code = runCmd("send", "--config", cfgPath, "--server", "DevServer", "test_queue", `{"hello":"delay-until"}`, "--delay-until", delayUntil, "-o", "json")
	if code != 0 {
		t.Fatalf("send with delay-until failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	var delayedUntilIDs []int64
	if err := json.Unmarshal([]byte(out), &delayedUntilIDs); err != nil || len(delayedUntilIDs) == 0 {
		t.Fatalf("expected delay-until send IDs, got %q", out)
	}

	out, errOut, code = runCmd("read", "--config", cfgPath, "--server", "DevServer", "test_queue", "--vt", "30", "--qty", "1", "-o", "json")
	if code != 0 {
		t.Fatalf("read failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	var readObj map[string]any
	if err := json.Unmarshal([]byte(out), &readObj); err != nil || len(readObj) == 0 {
		t.Fatalf("expected read record, got %q", out)
	}

	out, errOut, code = runCmd("delete", "--config", cfgPath, "--server", "DevServer", "test_queue", fmt.Sprintf("%d", sendIDs[0]), "-o", "json")
	if code != 0 {
		t.Fatalf("delete failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	var deleted []int64
	if err := json.Unmarshal([]byte(out), &deleted); err != nil || len(deleted) == 0 {
		t.Fatalf("expected delete ids, got %q", out)
	}

	out, errOut, code = runCmd("send", "--config", cfgPath, "--server", "DevServer", "test_queue", `{"hello":"pop"}`, "-o", "json")
	if code != 0 {
		t.Fatalf("send for pop failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}

	out, errOut, code = runCmd("pop", "--config", cfgPath, "--server", "DevServer", "test_queue", "--qty", "1", "-o", "json")
	if code != 0 {
		t.Fatalf("pop failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	var popped map[string]any
	if err := json.Unmarshal([]byte(out), &popped); err != nil || len(popped) == 0 {
		t.Fatalf("expected pop record, got %q", out)
	}

	out, errOut, code = runCmd("send", "--config", cfgPath, "--server", "DevServer", "test_queue", `{"hello":"message"}`, "-o", "json")
	if code != 0 {
		t.Fatalf("send for message output failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}

	out, errOut, code = runCmd("pop", "--config", cfgPath, "--server", "DevServer", "test_queue", "--qty", "1", "-o", "message")
	if code != 0 {
		t.Fatalf("pop message failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	var poppedMessage map[string]any
	if err := json.Unmarshal([]byte(out), &poppedMessage); err != nil || len(poppedMessage) == 0 {
		t.Fatalf("expected pop message, got %q", out)
	}

	out, errOut, code = runCmd("send", "--config", cfgPath, "--server", "DevServer", "test_queue", `{"hello":"message1"}`, "-o", "json")
	if code != 0 {
		t.Fatalf("send for message1 failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	out, errOut, code = runCmd("send", "--config", cfgPath, "--server", "DevServer", "test_queue", `{"hello":"message2"}`, "-o", "json")
	if code != 0 {
		t.Fatalf("send for message2 failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}

	out, errOut, code = runCmd("pop", "--config", cfgPath, "--server", "DevServer", "test_queue", "--qty", "2", "-o", "message")
	if code != 0 {
		t.Fatalf("pop message qty=2 failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	var poppedMessages []map[string]any
	if err := json.Unmarshal([]byte(out), &poppedMessages); err != nil || len(poppedMessages) != 2 {
		t.Fatalf("expected pop messages array, got %q", out)
	}

	out, errOut, code = runCmd("send", "--config", cfgPath, "--server", "DevServer", "test_queue", `{"hello":"archive"}`, "-o", "json")
	if code != 0 {
		t.Fatalf("send for archive failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	var archiveIDs []int64
	if err := json.Unmarshal([]byte(out), &archiveIDs); err != nil || len(archiveIDs) == 0 {
		t.Fatalf("expected archive IDs, got %q", out)
	}

	out, errOut, code = runCmd("archive", "--config", cfgPath, "--server", "DevServer", "test_queue", fmt.Sprintf("%d", archiveIDs[0]), "-o", "json")
	if code != 0 {
		t.Fatalf("archive failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	var archived []int64
	if err := json.Unmarshal([]byte(out), &archived); err != nil || len(archived) == 0 {
		t.Fatalf("expected archived ids, got %q", out)
	}

	out, errOut, code = runCmd("send", "--config", cfgPath, "--server", "DevServer", "test_queue", `{"hello":"purge"}`, "-o", "json")
	if code != 0 {
		t.Fatalf("send for purge failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}

	out, errOut, code = runCmd("purge", "--config", cfgPath, "--server", "DevServer", "test_queue", "--yes")
	if code != 0 {
		t.Fatalf("purge failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if !strings.Contains(out, "queue purged") {
		t.Fatalf("unexpected purge output: %q", out)
	}

	out, errOut, code = runCmd("purge", "--config", cfgPath, "--server", "DevServer", "test_queue", "--yes")
	if code != 0 {
		t.Fatalf("purge (empty) failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if !strings.Contains(out, "queue is empty") {
		t.Fatalf("unexpected purge (empty) output: %q", out)
	}

	out, errOut, code = runCmd("drop", "--config", cfgPath, "--server", "DevServer", "test_queue", "--yes")
	if code != 0 {
		t.Fatalf("drop failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if !strings.Contains(out, "queue dropped") {
		t.Fatalf("unexpected drop output: %q", out)
	}

	out, errOut, code = runCmd("metrics", "--config", cfgPath, "--server", "DevServer", "test_queue")
	if code != 3 {
		t.Fatalf("metrics on missing queue expected code=3, got code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if !strings.Contains(strings.ToLower(errOut), "queue") {
		t.Fatalf("expected queue not found message, got stderr=%q", errOut)
	}

	out, errOut, code = runCmd("metrics", "--config", cfgPath, "--server", "DevServer")
	if code != 0 {
		t.Fatalf("metrics all failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}

	out, errOut, code = runCmd("read", "--config", cfgPath, "--server", "DevServer", "test_queue", "--vt", "30", "--qty", "1")
	if code != 3 {
		t.Fatalf("read on missing queue expected code=3, got code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if !strings.Contains(strings.ToLower(errOut), "queue") {
		t.Fatalf("expected queue not found message, got stderr=%q", errOut)
	}

	out, errOut, code = runCmd("pop", "--config", cfgPath, "--server", "DevServer", "test_queue", "--qty", "1")
	if code != 3 {
		t.Fatalf("pop on missing queue expected code=3, got code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if !strings.Contains(strings.ToLower(errOut), "queue") {
		t.Fatalf("expected queue not found message, got stderr=%q", errOut)
	}

	out, errOut, code = runCmd("delete", "--config", cfgPath, "--server", "DevServer", "test_queue", "1")
	if code != 3 {
		t.Fatalf("delete on missing queue expected code=3, got code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if !strings.Contains(strings.ToLower(errOut), "queue") {
		t.Fatalf("expected queue not found message, got stderr=%q", errOut)
	}

	out, errOut, code = runCmd("archive", "--config", cfgPath, "--server", "DevServer", "test_queue", "1")
	if code != 3 {
		t.Fatalf("archive on missing queue expected code=3, got code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if !strings.Contains(strings.ToLower(errOut), "queue") {
		t.Fatalf("expected queue not found message, got stderr=%q", errOut)
	}

	out, errOut, code = runCmd("purge", "--config", cfgPath, "--server", "DevServer", "test_queue", "--yes")
	if code != 3 {
		t.Fatalf("purge on missing queue expected code=3, got code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if !strings.Contains(strings.ToLower(errOut), "queue") {
		t.Fatalf("expected queue not found message, got stderr=%q", errOut)
	}

	out, errOut, code = runCmd("create", "--config", cfgPath, "--server", "DevServer", "test_queue_json")
	if code != 0 {
		t.Fatalf("create json failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
}

func TestTopicRouting(t *testing.T) {
	ctx := context.Background()

	container, host, port := startPGMQContainer(t, ctx)
	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	bin := buildBinary(t)
	home := setupHome(t, host, port)
	cfgPath := filepath.Join(home, ".pgmq", "config.json")

	out, errOut, code := runCLICommand(t, bin, home, "--config", cfgPath, "--server", "DevServer", "init")
	if code != 0 {
		t.Fatalf("init failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if !topicRoutingSupported(t, host, port) {
		t.Skip("topic routing requires pgmq 1.11.0 or later")
	}

	for _, queue := range []string{"all_logs", "error_logs", "api_errors"} {
		out, errOut, code = runCLICommand(t, bin, home, "--config", cfgPath, "--server", "DevServer", "create", queue)
		if code != 0 {
			t.Fatalf("create %s failed: code=%d stdout=%q stderr=%q", queue, code, out, errOut)
		}
	}

	bindings := []struct {
		pattern string
		queue   string
	}{
		{pattern: "logs.#", queue: "all_logs"},
		{pattern: "logs.*.error", queue: "error_logs"},
		{pattern: "logs.api.error", queue: "api_errors"},
	}
	for _, binding := range bindings {
		out, errOut, code = runCLICommand(t, bin, home, "--config", cfgPath, "--server", "DevServer", "topic", "bind", binding.pattern, binding.queue)
		if code != 0 {
			t.Fatalf("topic bind %s -> %s failed: code=%d stdout=%q stderr=%q", binding.pattern, binding.queue, code, out, errOut)
		}
		if !strings.Contains(out, "topic bound") {
			t.Fatalf("unexpected topic bind output: %q", out)
		}
	}

	out, errOut, code = runCLICommand(t, bin, home, "--config", cfgPath, "--server", "DevServer", "topic", "list", "-o", "json")
	if code != 0 {
		t.Fatalf("topic list all failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	var allBindings []topicBindingResult
	if err := json.Unmarshal([]byte(out), &allBindings); err != nil {
		t.Fatalf("expected topic list json array, got %q", out)
	}
	if len(allBindings) != 3 {
		t.Fatalf("expected 3 topic bindings, got %#v", allBindings)
	}

	out, errOut, code = runCLICommand(t, bin, home, "--config", cfgPath, "--server", "DevServer", "topic", "list", "api_errors", "-o", "json")
	if code != 0 {
		t.Fatalf("topic list queue failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	var apiBindings []topicBindingResult
	if err := json.Unmarshal([]byte(out), &apiBindings); err != nil {
		t.Fatalf("expected topic list queue json array, got %q", out)
	}
	if len(apiBindings) != 1 || apiBindings[0].Pattern != "logs.api.error" || apiBindings[0].QueueName != "api_errors" {
		t.Fatalf("unexpected api_errors bindings: %#v", apiBindings)
	}

	out, errOut, code = runCLICommand(t, bin, home, "--config", cfgPath, "--server", "DevServer", "topic", "test", "logs.api.error", "-o", "json")
	if code != 0 {
		t.Fatalf("topic test failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	var routed []topicRouteResult
	if err := json.Unmarshal([]byte(out), &routed); err != nil {
		t.Fatalf("expected topic test json array, got %q", out)
	}
	if len(routed) != 3 {
		t.Fatalf("expected 3 matching routes, got %#v", routed)
	}
	gotQueues := map[string]bool{}
	gotPatterns := map[string]bool{}
	for _, route := range routed {
		gotQueues[route.QueueName] = true
		gotPatterns[route.Pattern] = true
	}
	for _, queue := range []string{"all_logs", "error_logs", "api_errors"} {
		if !gotQueues[queue] {
			t.Fatalf("expected routed queues to include %q, got %#v", queue, routed)
		}
	}
	for _, pattern := range []string{"logs.#", "logs.*.error", "logs.api.error"} {
		if !gotPatterns[pattern] {
			t.Fatalf("expected routed patterns to include %q, got %#v", pattern, routed)
		}
	}

	out, errOut, code = runCLICommand(t, bin, home, "--config", cfgPath, "--server", "DevServer", "topic", "send", "logs.api.error", `{"message":"first"}`, "-o", "json")
	if code != 0 {
		t.Fatalf("topic send failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	var firstSend []topicSendResult
	if err := json.Unmarshal([]byte(out), &firstSend); err != nil {
		t.Fatalf("expected topic send json array, got %q", out)
	}
	if len(firstSend) != 3 {
		t.Fatalf("expected 3 topic send results, got %#v", firstSend)
	}
	gotSendQueues := map[string]bool{}
	for _, sent := range firstSend {
		gotSendQueues[sent.QueueName] = true
		if sent.MsgID <= 0 {
			t.Fatalf("expected positive msg_id, got %#v", firstSend)
		}
	}
	for _, queue := range []string{"all_logs", "error_logs", "api_errors"} {
		if !gotSendQueues[queue] {
			t.Fatalf("expected send results to include %q, got %#v", queue, firstSend)
		}
	}

	for _, queue := range []string{"all_logs", "error_logs", "api_errors"} {
		out, errOut, code = runCLICommand(t, bin, home, "--config", cfgPath, "--server", "DevServer", "read", queue, "--vt", "30", "--qty", "1", "-o", "json")
		if code != 0 {
			t.Fatalf("read %s failed: code=%d stdout=%q stderr=%q", queue, code, out, errOut)
		}
		var rec queueReadResult
		if err := json.Unmarshal([]byte(out), &rec); err != nil {
			t.Fatalf("expected read json object, got %q", out)
		}
		if rec.Message["message"] != "first" {
			t.Fatalf("expected first message in %s, got %#v", queue, rec.Message)
		}
	}

	out, errOut, code = runCLICommand(t, bin, home, "--config", cfgPath, "--server", "DevServer", "topic", "test", "orders.created", "-o", "json")
	if code != 0 {
		t.Fatalf("topic test unmatched failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	var unmatchedRoutes []any
	if err := json.Unmarshal([]byte(out), &unmatchedRoutes); err != nil || len(unmatchedRoutes) != 0 {
		t.Fatalf("expected empty route result, got %q", out)
	}

	out, errOut, code = runCLICommand(t, bin, home, "--config", cfgPath, "--server", "DevServer", "topic", "send", "orders.created", `{"message":"ignored"}`, "-o", "json")
	if code != 0 {
		t.Fatalf("topic send unmatched failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	var unmatchedSend []any
	if err := json.Unmarshal([]byte(out), &unmatchedSend); err != nil || len(unmatchedSend) != 0 {
		t.Fatalf("expected empty send result, got %q", out)
	}

	out, errOut, code = runCLICommand(t, bin, home, "--config", cfgPath, "--server", "DevServer", "topic", "unbind", "logs.api.error", "api_errors")
	if code != 0 {
		t.Fatalf("topic unbind failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if !strings.Contains(out, "topic unbound") {
		t.Fatalf("unexpected topic unbind output: %q", out)
	}

	out, errOut, code = runCLICommand(t, bin, home, "--config", cfgPath, "--server", "DevServer", "topic", "send", "logs.api.error", `{"message":"second"}`, "-o", "json")
	if code != 0 {
		t.Fatalf("topic send after unbind failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	var secondSend []topicSendResult
	if err := json.Unmarshal([]byte(out), &secondSend); err != nil {
		t.Fatalf("expected topic send json array after unbind, got %q", out)
	}
	if len(secondSend) != 2 {
		t.Fatalf("expected 2 topic send results after unbind, got %#v", secondSend)
	}
	gotSecondQueues := map[string]bool{}
	for _, sent := range secondSend {
		gotSecondQueues[sent.QueueName] = true
	}
	if !gotSecondQueues["all_logs"] || !gotSecondQueues["error_logs"] || gotSecondQueues["api_errors"] {
		t.Fatalf("unexpected queues after unbind: %#v", secondSend)
	}

	for _, queue := range []string{"all_logs", "error_logs"} {
		out, errOut, code = runCLICommand(t, bin, home, "--config", cfgPath, "--server", "DevServer", "read", queue, "--vt", "30", "--qty", "1", "-o", "json")
		if code != 0 {
			t.Fatalf("read %s after unbind failed: code=%d stdout=%q stderr=%q", queue, code, out, errOut)
		}
		var rec queueReadResult
		if err := json.Unmarshal([]byte(out), &rec); err != nil {
			t.Fatalf("expected read json object after unbind, got %q", out)
		}
		if rec.Message["message"] != "second" {
			t.Fatalf("expected second message in %s, got %#v", queue, rec.Message)
		}
	}

	out, errOut, code = runCLICommand(t, bin, home, "--config", cfgPath, "--server", "DevServer", "read", "api_errors", "--vt", "30", "--qty", "1", "-o", "json")
	if code != 0 {
		t.Fatalf("read api_errors after unbind failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	var apiRead any
	if err := json.Unmarshal([]byte(out), &apiRead); err != nil {
		t.Fatalf("expected read api_errors json null, got %q", out)
	}
	if apiRead != nil {
		t.Fatalf("expected no message in api_errors after unbind, got %#v", apiRead)
	}
}

func TestFIFOGroupedReadAndIndexes(t *testing.T) {
	ctx := context.Background()

	container, host, port := startPGMQContainer(t, ctx)
	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	bin := buildBinary(t)
	home := setupHome(t, host, port)
	cfgPath := filepath.Join(home, ".pgmq", "config.json")

	out, errOut, code := runCLICommand(t, bin, home, "--config", cfgPath, "--server", "DevServer", "init")
	if code != 0 {
		t.Fatalf("init failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if !fifoSupported(t, host, port) {
		t.Skip("FIFO grouped reads require pgmq FIFO functions")
	}

	out, errOut, code = runCLICommand(t, bin, home, "--config", cfgPath, "--server", "DevServer", "create", "fifo_all")
	if code != 0 {
		t.Fatalf("create fifo_all failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	out, errOut, code = runCLICommand(t, bin, home, "--config", cfgPath, "--server", "DevServer", "fifo", "index", "--all")
	if code != 0 {
		t.Fatalf("fifo index --all failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if !strings.Contains(out, "fifo indexes created") {
		t.Fatalf("unexpected fifo index --all output: %q", out)
	}

	out, errOut, code = runCLICommand(t, bin, home, "--config", cfgPath, "--server", "DevServer", "create", "fifo_single")
	if code != 0 {
		t.Fatalf("create fifo_single failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	out, errOut, code = runCLICommand(t, bin, home, "--config", cfgPath, "--server", "DevServer", "fifo", "index", "fifo_single")
	if code != 0 {
		t.Fatalf("fifo index queue failed: code=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if !strings.Contains(out, "fifo index created") {
		t.Fatalf("unexpected fifo index output: %q", out)
	}

	tests := []struct {
		strategy   string
		queue      string
		wantLabels []string
	}{
		{
			strategy:   "grouped",
			queue:      "fifo_grouped",
			wantLabels: []string{"a1", "a2"},
		},
		{
			strategy:   "grouped-rr",
			queue:      "fifo_grouped_rr",
			wantLabels: []string{"a1", "b1"},
		},
		{
			strategy:   "grouped-head",
			queue:      "fifo_grouped_head",
			wantLabels: []string{"a1", "b1"},
		},
	}

	for _, tt := range tests {
		out, errOut, code = runCLICommand(t, bin, home, "--config", cfgPath, "--server", "DevServer", "create", tt.queue)
		if code != 0 {
			t.Fatalf("create %s failed: code=%d stdout=%q stderr=%q", tt.queue, code, out, errOut)
		}

		messages := []struct {
			group string
			label string
		}{
			{group: "a", label: "a1"},
			{group: "a", label: "a2"},
			{group: "b", label: "b1"},
			{group: "b", label: "b2"},
		}
		for _, msg := range messages {
			body := fmt.Sprintf(`{"label":%q}`, msg.label)
			headers := fmt.Sprintf(`{"x-pgmq-group":%q}`, msg.group)
			out, errOut, code = runCLICommand(t, bin, home, "--config", cfgPath, "--server", "DevServer", "send", tt.queue, body, "--headers", headers, "-o", "json")
			if code != 0 {
				t.Fatalf("send %s to %s failed: code=%d stdout=%q stderr=%q", msg.label, tt.queue, code, out, errOut)
			}
		}

		out, errOut, code = runCLICommand(t, bin, home, "--config", cfgPath, "--server", "DevServer", "read", tt.queue, "--vt", "30", "--qty", "2", "--strategy", tt.strategy, "-o", "json")
		if code != 0 {
			t.Fatalf("read %s failed: code=%d stdout=%q stderr=%q", tt.strategy, code, out, errOut)
		}
		var records []queueReadResult
		if err := json.Unmarshal([]byte(out), &records); err != nil {
			t.Fatalf("expected read json array for %s, got %q", tt.strategy, out)
		}
		if len(records) != len(tt.wantLabels) {
			t.Fatalf("expected %d %s records, got %#v", len(tt.wantLabels), tt.strategy, records)
		}

		gotLabels := make([]string, 0, len(records))
		for _, rec := range records {
			label, ok := rec.Message["label"].(string)
			if !ok {
				t.Fatalf("expected string label in %s record, got %#v", tt.strategy, rec.Message)
			}
			gotLabels = append(gotLabels, label)
		}
		if !sameStringElements(gotLabels, tt.wantLabels) {
			t.Fatalf("unexpected %s labels: got %v want %v", tt.strategy, gotLabels, tt.wantLabels)
		}
	}
}

func sameStringElements(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	counts := map[string]int{}
	for _, item := range got {
		counts[item]++
	}
	for _, item := range want {
		counts[item]--
		if counts[item] < 0 {
			return false
		}
	}
	return true
}

func startPGMQContainer(t *testing.T, ctx context.Context) (testcontainers.Container, string, string) {
	t.Helper()

	image := os.Getenv("PGMQ_TEST_IMAGE")
	if image == "" {
		image = "ghcr.io/pgmq/pg17-pgmq:v1.11.1"
	}

	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       "pgmq",
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("5432/tcp"),
			wait.ForExec([]string{"pg_isready", "-U", "postgres", "-d", "pgmq"}),
		).WithDeadline(2 * time.Minute),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("container host: %v", err)
	}
	mappedPort, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatalf("mapped port: %v", err)
	}
	if host == "" {
		host = "localhost"
	}

	port := mappedPort.Port()
	if _, err := net.LookupHost(host); err != nil {
		t.Fatalf("container host lookup failed: %v", err)
	}

	return container, host, port
}

func setupHome(t *testing.T, host, port string) string {
	t.Helper()

	home := t.TempDir()
	cfgDir := filepath.Join(home, ".pgmq")
	if err := os.MkdirAll(cfgDir, 0o755); err != nil {
		t.Fatalf("mkdir config dir: %v", err)
	}
	cfgPath := filepath.Join(cfgDir, "config.json")
	conn := fmt.Sprintf("Host=%s;Port=%s;Username=postgres;Password=postgres;Database=pgmq;sslmode=disable", host, port)
	cfg := fmt.Sprintf(`{
  "defaultServer": "DevServer",
  "servers": {
    "DevServer": {
      "connectionString": %q
    }
  }
}
`, conn)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return home
}

func runCLICommand(t *testing.T, bin, home string, args ...string) (string, string, int) {
	t.Helper()

	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), "HOME="+home)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		return stdout.String(), stderr.String(), 0
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		return stdout.String(), stderr.String(), exitErr.ExitCode()
	}
	t.Fatalf("command failed to run: %v", err)
	return "", "", -1
}

func topicRoutingSupported(t *testing.T, host, port string) bool {
	t.Helper()

	ctx := context.Background()
	connStr := fmt.Sprintf("host=%s port=%s user=postgres password=postgres dbname=pgmq sslmode=disable", host, port)
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("connect for topic capability check: %v", err)
	}
	defer conn.Close(ctx)

	var supported bool
	if err := conn.QueryRow(ctx, "SELECT to_regprocedure('pgmq.bind_topic(text,text)') IS NOT NULL;").Scan(&supported); err != nil {
		t.Fatalf("topic capability check failed: %v", err)
	}
	return supported
}

func fifoSupported(t *testing.T, host, port string) bool {
	t.Helper()

	ctx := context.Background()
	connStr := fmt.Sprintf("host=%s port=%s user=postgres password=postgres dbname=pgmq sslmode=disable", host, port)
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("connect for FIFO capability check: %v", err)
	}
	defer conn.Close(ctx)

	var supported bool
	if err := conn.QueryRow(ctx, `SELECT
		to_regprocedure('pgmq.read_grouped(text,integer,integer)') IS NOT NULL
		AND to_regprocedure('pgmq.read_grouped_rr(text,integer,integer)') IS NOT NULL
		AND to_regprocedure('pgmq.read_grouped_head(text,integer,integer)') IS NOT NULL
		AND to_regprocedure('pgmq.create_fifo_index(text)') IS NOT NULL
		AND to_regprocedure('pgmq.create_fifo_indexes_all()') IS NOT NULL;`).Scan(&supported); err != nil {
		t.Fatalf("FIFO capability check failed: %v", err)
	}
	return supported
}

func TestSetupHomeUsesProvidedHost(t *testing.T) {
	host := "example.internal"
	port := "5432"

	home := setupHome(t, host, port)
	cfgPath := filepath.Join(home, ".pgmq", "config.json")
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if !strings.Contains(string(data), "Host="+host) {
		t.Fatalf("expected config to contain host %q, got: %s", host, string(data))
	}
}

func buildBinary(t *testing.T) string {
	t.Helper()

	root := findRepoRoot(t)
	outPath := filepath.Join(t.TempDir(), "pgmq")
	cmd := exec.Command("go", "build", "-o", outPath, "./cmd/pgmq")
	cmd.Dir = root
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build failed: %v, output=%s", err, string(out))
	}
	return outPath
}

func findRepoRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not find go.mod from %s", dir)
		}
		dir = parent
	}
}
