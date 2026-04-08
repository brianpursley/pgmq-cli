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
	"context"
	"encoding/json"
	"time"

	"github.com/spf13/cobra"

	"pgmq-cli/internal/errs"
	"pgmq-cli/internal/output"
)

func SendCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "send <queue> <message-json>",
		Short: "Send a message",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runSend(cmd, args[0], args[1])
		},
	}
	cmd.Flags().String("headers", "", "JSON headers")
	cmd.Flags().Int("delay", 0, "Delay in seconds")
	cmd.Flags().String("delay-until", "", "Delay until RFC3339 timestamp")
	addOutputFlag(cmd, false)
	cmd.ValidArgsFunction = queueNameCompletion
	return cmd
}

func runSend(cmd *cobra.Command, queue, message string) error {
	var msg json.RawMessage
	if err := json.Unmarshal([]byte(message), &msg); err != nil {
		return errs.NewUsageError("message must be valid JSON")
	}

	headersStr, err := cmd.Flags().GetString("headers")
	if err != nil {
		return errs.NewUsageError("failed to read --headers flag")
	}
	var headers json.RawMessage
	if headersStr != "" {
		if err := json.Unmarshal([]byte(headersStr), &headers); err != nil {
			return errs.NewUsageError("headers must be valid JSON")
		}
	}

	delaySeconds, err := cmd.Flags().GetInt("delay")
	if err != nil {
		return errs.NewUsageError("failed to read --delay flag")
	}
	if delaySeconds < 0 {
		return errs.NewUsageError("--delay must be >= 0")
	}
	delayAtStr, err := cmd.Flags().GetString("delay-until")
	if err != nil {
		return errs.NewUsageError("failed to read --delay-until flag")
	}
	if delaySeconds > 0 && delayAtStr != "" {
		return errs.NewUsageError("--delay and --delay-until are mutually exclusive")
	}

	var delayAt time.Time
	if delayAtStr != "" {
		parsed, err := time.Parse(time.RFC3339, delayAtStr)
		if err != nil {
			return errs.NewUsageError("--delay-until must be RFC3339")
		}
		delayAt = parsed
	}

	conn, _, err := connect(cmd)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Close(ctx)

	var rows rowsScanner
	switch {
	case headersStr != "" && delayAtStr != "":
		rows, err = queryRows(ctx, conn, "SELECT pgmq.send($1::text, $2::jsonb, $3::jsonb, $4::timestamptz) AS msg_id;", queue, msg, headers, delayAt)
	case headersStr != "" && delaySeconds > 0:
		rows, err = queryRows(ctx, conn, "SELECT pgmq.send($1::text, $2::jsonb, $3::jsonb, $4::integer) AS msg_id;", queue, msg, headers, delaySeconds)
	case headersStr != "":
		rows, err = queryRows(ctx, conn, "SELECT pgmq.send($1::text, $2::jsonb, $3::jsonb) AS msg_id;", queue, msg, headers)
	case delayAtStr != "":
		rows, err = queryRows(ctx, conn, "SELECT pgmq.send($1::text, $2::jsonb, $3::timestamptz) AS msg_id;", queue, msg, delayAt)
	case delaySeconds > 0:
		rows, err = queryRows(ctx, conn, "SELECT pgmq.send($1::text, $2::jsonb, $3::integer) AS msg_id;", queue, msg, delaySeconds)
	default:
		rows, err = queryRows(ctx, conn, "SELECT pgmq.send($1::text, $2::jsonb) AS msg_id;", queue, msg)
	}
	if err != nil {
		return dbErrorForQueue(err, queue)
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return dbError(err)
		}
		ids = append(ids, id)
	}
	if rows.Err() != nil {
		return dbErrorForQueue(rows.Err(), queue)
	}
	if len(ids) == 0 {
		return outputEmpty(cmd, "no messages sent")
	}

	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}
	if format == "json" {
		return output.PrintJSON(cmd.OutOrStdout(), ids)
	}

	headersTable := []string{"msg_id"}
	tableRows := make([][]string, 0, len(ids))
	for _, id := range ids {
		tableRows = append(tableRows, []string{int64String(id)})
	}
	output.PrintTable(cmd.OutOrStdout(), headersTable, tableRows)
	return nil
}
