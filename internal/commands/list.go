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

type queueRecord struct {
	Name          string    `json:"queue_name"`
	CreatedAt     time.Time `json:"created_at"`
	IsPartitioned bool      `json:"is_partitioned"`
	IsUnlogged    bool      `json:"is_unlogged"`
}

func ListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List queues",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runListQueues(cmd)
		},
	}
	addOutputFlag(cmd, false)
	return cmd
}

func runListQueues(cmd *cobra.Command) error {
	conn, _, err := connect(cmd)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Close(ctx)

	rows, err := conn.Conn.Query(ctx, "SELECT queue_name, is_partitioned, is_unlogged, created_at FROM pgmq.list_queues();")
	if err != nil {
		return dbError(err)
	}
	defer rows.Close()

	var records []queueRecord
	for rows.Next() {
		var rec queueRecord
		if err := rows.Scan(&rec.Name, &rec.IsPartitioned, &rec.IsUnlogged, &rec.CreatedAt); err != nil {
			return dbError(err)
		}
		records = append(records, rec)
	}
	if rows.Err() != nil {
		return dbError(rows.Err())
	}
	headers := []string{"queue_name", "created_at", "is_partitioned", "is_unlogged"}
	tableRows := make([][]string, 0, len(records))
	for _, rec := range records {
		tableRows = append(tableRows, []string{
			rec.Name,
			rec.CreatedAt.Format(time.RFC3339),
			boolString(rec.IsPartitioned),
			boolString(rec.IsUnlogged),
		})
	}
	return renderOutput(cmd, headers, tableRows, records, "no queues found")
}
