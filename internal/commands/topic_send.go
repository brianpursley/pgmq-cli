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
)

type topicSendRecord struct {
	QueueName string `json:"queue_name"`
	MsgID     int64  `json:"msg_id"`
}

type topicSendOptions struct {
	Message      json.RawMessage
	Headers      json.RawMessage
	HasHeaders   bool
	DelaySeconds int
	DelayAt      time.Time
	HasDelayAt   bool
}

func TopicSendCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "send <routing-key> <message-json>",
		Short: "Send a message using topic routing",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTopicSend(cmd, args[0], args[1])
		},
	}
	cmd.Flags().String("headers", "", "JSON headers")
	cmd.Flags().Int("delay", 0, "Delay in seconds")
	cmd.Flags().String("delay-until", "", "Delay until RFC3339 timestamp")
	addOutputFlag(cmd, false)
	return cmd
}

func runTopicSend(cmd *cobra.Command, routingKey, message string) error {
	opts, err := parseTopicSendOptions(cmd, message)
	if err != nil {
		return err
	}

	conn, _, err := connect(cmd)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Close(ctx)

	sql, args := buildTopicSendBatchQuery(routingKey, opts)
	rows, err := queryRows(ctx, conn, sql, args...)
	if err != nil {
		return dbErrorForTopic(err)
	}
	defer rows.Close()

	var records []topicSendRecord
	for rows.Next() {
		var rec topicSendRecord
		if err := rows.Scan(&rec.QueueName, &rec.MsgID); err != nil {
			return dbError(err)
		}
		records = append(records, rec)
	}
	if rows.Err() != nil {
		return dbErrorForTopic(rows.Err())
	}

	return renderTopicSendOutput(cmd, records)
}

func parseTopicSendOptions(cmd *cobra.Command, message string) (topicSendOptions, error) {
	var opts topicSendOptions

	if err := json.Unmarshal([]byte(message), &opts.Message); err != nil {
		return topicSendOptions{}, errs.NewUsageError("message must be valid JSON")
	}

	headersStr, err := cmd.Flags().GetString("headers")
	if err != nil {
		return topicSendOptions{}, errs.NewUsageError("failed to read --headers flag")
	}
	if headersStr != "" {
		if err := json.Unmarshal([]byte(headersStr), &opts.Headers); err != nil {
			return topicSendOptions{}, errs.NewUsageError("headers must be valid JSON")
		}
		opts.HasHeaders = true
	}

	delaySeconds, err := cmd.Flags().GetInt("delay")
	if err != nil {
		return topicSendOptions{}, errs.NewUsageError("failed to read --delay flag")
	}
	if delaySeconds < 0 {
		return topicSendOptions{}, errs.NewUsageError("--delay must be >= 0")
	}
	opts.DelaySeconds = delaySeconds

	delayAtStr, err := cmd.Flags().GetString("delay-until")
	if err != nil {
		return topicSendOptions{}, errs.NewUsageError("failed to read --delay-until flag")
	}
	if delaySeconds > 0 && delayAtStr != "" {
		return topicSendOptions{}, errs.NewUsageError("--delay and --delay-until are mutually exclusive")
	}
	if delayAtStr != "" {
		parsed, err := time.Parse(time.RFC3339, delayAtStr)
		if err != nil {
			return topicSendOptions{}, errs.NewUsageError("--delay-until must be RFC3339")
		}
		opts.DelayAt = parsed
		opts.HasDelayAt = true
	}

	return opts, nil
}

func buildTopicSendBatchQuery(routingKey string, opts topicSendOptions) (string, []any) {
	switch {
	case opts.HasHeaders && opts.HasDelayAt:
		return "SELECT queue_name, msg_id FROM pgmq.send_batch_topic($1::text, ARRAY[$2::jsonb], ARRAY[$3::jsonb], $4::timestamptz);", []any{routingKey, opts.Message, opts.Headers, opts.DelayAt}
	case opts.HasHeaders && opts.DelaySeconds > 0:
		return "SELECT queue_name, msg_id FROM pgmq.send_batch_topic($1::text, ARRAY[$2::jsonb], ARRAY[$3::jsonb], $4::integer);", []any{routingKey, opts.Message, opts.Headers, opts.DelaySeconds}
	case opts.HasHeaders:
		return "SELECT queue_name, msg_id FROM pgmq.send_batch_topic($1::text, ARRAY[$2::jsonb], ARRAY[$3::jsonb]);", []any{routingKey, opts.Message, opts.Headers}
	case opts.HasDelayAt:
		return "SELECT queue_name, msg_id FROM pgmq.send_batch_topic($1::text, ARRAY[$2::jsonb], $3::timestamptz);", []any{routingKey, opts.Message, opts.DelayAt}
	case opts.DelaySeconds > 0:
		return "SELECT queue_name, msg_id FROM pgmq.send_batch_topic($1::text, ARRAY[$2::jsonb], $3::integer);", []any{routingKey, opts.Message, opts.DelaySeconds}
	default:
		return "SELECT queue_name, msg_id FROM pgmq.send_batch_topic($1::text, ARRAY[$2::jsonb]);", []any{routingKey, opts.Message}
	}
}

func renderTopicSendOutput(cmd *cobra.Command, records []topicSendRecord) error {
	headers := []string{"queue_name", "msg_id"}
	tableRows := make([][]string, 0, len(records))
	for _, rec := range records {
		tableRows = append(tableRows, []string{
			rec.QueueName,
			int64String(rec.MsgID),
		})
	}
	return renderOutput(cmd, headers, tableRows, records, "no matching topic bindings")
}
