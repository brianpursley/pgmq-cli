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
	"time"

	"github.com/spf13/cobra"
)

type topicBindingRecord struct {
	Pattern       string    `json:"pattern"`
	QueueName     string    `json:"queue_name"`
	BoundAt       time.Time `json:"bound_at"`
	CompiledRegex string    `json:"compiled_regex"`
}

func TopicListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list [queue]",
		Short: "List topic bindings",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return runTopicListAll(cmd)
			}
			return runTopicList(cmd, args[0])
		},
	}
	addOutputFlag(cmd, false)
	cmd.ValidArgsFunction = queueNameCompletion
	return cmd
}

func runTopicListAll(cmd *cobra.Command) error {
	conn, _, err := connect(cmd)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Close(ctx)

	rows, err := queryRows(ctx, conn, "SELECT pattern, queue_name, bound_at, compiled_regex FROM pgmq.list_topic_bindings();")
	if err != nil {
		return dbErrorForTopic(err)
	}
	defer rows.Close()

	var records []topicBindingRecord
	for rows.Next() {
		var rec topicBindingRecord
		if err := rows.Scan(&rec.Pattern, &rec.QueueName, &rec.BoundAt, &rec.CompiledRegex); err != nil {
			return dbError(err)
		}
		records = append(records, rec)
	}
	if rows.Err() != nil {
		return dbErrorForTopic(rows.Err())
	}

	return renderTopicBindingOutput(cmd, records)
}

func runTopicList(cmd *cobra.Command, queue string) error {
	conn, _, err := connect(cmd)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Close(ctx)

	rows, err := queryRows(ctx, conn, "SELECT pattern, queue_name, bound_at, compiled_regex FROM pgmq.list_topic_bindings($1::text);", queue)
	if err != nil {
		return dbErrorForTopicQueue(err, queue)
	}
	defer rows.Close()

	var records []topicBindingRecord
	for rows.Next() {
		var rec topicBindingRecord
		if err := rows.Scan(&rec.Pattern, &rec.QueueName, &rec.BoundAt, &rec.CompiledRegex); err != nil {
			return dbError(err)
		}
		records = append(records, rec)
	}
	if rows.Err() != nil {
		return dbErrorForTopicQueue(rows.Err(), queue)
	}

	return renderTopicBindingOutput(cmd, records)
}

func renderTopicBindingOutput(cmd *cobra.Command, records []topicBindingRecord) error {
	headers := []string{"pattern", "queue_name", "bound_at", "compiled_regex"}
	tableRows := make([][]string, 0, len(records))
	for _, rec := range records {
		tableRows = append(tableRows, []string{
			rec.Pattern,
			rec.QueueName,
			rec.BoundAt.Format(time.RFC3339),
			rec.CompiledRegex,
		})
	}
	return renderOutput(cmd, headers, tableRows, records, "no topic bindings found")
}
