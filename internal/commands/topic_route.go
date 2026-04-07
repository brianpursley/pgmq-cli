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

	"github.com/spf13/cobra"
)

type topicRouteRecord struct {
	Pattern       string `json:"pattern"`
	QueueName     string `json:"queue_name"`
	CompiledRegex string `json:"compiled_regex"`
}

func TopicTestCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "test <routing-key>",
		Short: "Test topic routing without sending a message",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTopicTest(cmd, args[0])
		},
	}
	addOutputFlag(cmd, false)
	return cmd
}

func runTopicTest(cmd *cobra.Command, routingKey string) error {
	conn, _, err := connect(cmd)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Close(ctx)

	rows, err := queryRows(ctx, conn, "SELECT pattern, queue_name, compiled_regex FROM pgmq.test_routing($1::text);", routingKey)
	if err != nil {
		return dbErrorForTopic(err)
	}
	defer rows.Close()

	var records []topicRouteRecord
	for rows.Next() {
		var rec topicRouteRecord
		if err := rows.Scan(&rec.Pattern, &rec.QueueName, &rec.CompiledRegex); err != nil {
			return dbError(err)
		}
		records = append(records, rec)
	}
	if rows.Err() != nil {
		return dbErrorForTopic(rows.Err())
	}

	return renderTopicRouteOutput(cmd, records)
}

func renderTopicRouteOutput(cmd *cobra.Command, records []topicRouteRecord) error {
	headers := []string{"pattern", "queue_name", "compiled_regex"}
	tableRows := make([][]string, 0, len(records))
	for _, rec := range records {
		tableRows = append(tableRows, []string{
			rec.Pattern,
			rec.QueueName,
			rec.CompiledRegex,
		})
	}
	return renderOutput(cmd, headers, tableRows, records, "no matching topic bindings")
}
