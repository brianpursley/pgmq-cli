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
	"database/sql"

	"github.com/spf13/cobra"
)

type metricsRecord struct {
	QueueName       string `json:"queue_name"`
	QueueLength     int64  `json:"queue_length"`
	NewestMsgAgeSec *int64 `json:"newest_msg_age_sec"`
	OldestMsgAgeSec *int64 `json:"oldest_msg_age_sec"`
}

func MetricsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "metrics [queue]",
		Short: "Queue metrics",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return runMetricsAll(cmd)
			}
			return runMetrics(cmd, args[0])
		},
	}
	addOutputFlag(cmd, false)
	cmd.ValidArgsFunction = queueNameCompletion
	return cmd
}

func runMetrics(cmd *cobra.Command, queue string) error {
	conn, _, err := connect(cmd)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Close(ctx)

	var newestMsgAgeSec sql.NullInt64
	var oldestMsgAgeSec sql.NullInt64
	var rec metricsRecord
	err = conn.Conn.QueryRow(ctx, "SELECT queue_name, queue_length, newest_msg_age_sec, oldest_msg_age_sec FROM pgmq.metrics($1);", queue).Scan(
		&rec.QueueName,
		&rec.QueueLength,
		&newestMsgAgeSec,
		&oldestMsgAgeSec,
	)
	if err != nil {
		return dbErrorForQueue(err, queue)
	}
	rec.NewestMsgAgeSec = nullInt64Ptr(newestMsgAgeSec)
	rec.OldestMsgAgeSec = nullInt64Ptr(oldestMsgAgeSec)

	headers := []string{"queue_name", "queue_length", "newest_msg_age_sec", "oldest_msg_age_sec"}
	row := []string{
		rec.QueueName,
		int64String(rec.QueueLength),
		optionalInt64String(rec.NewestMsgAgeSec),
		optionalInt64String(rec.OldestMsgAgeSec),
	}
	return renderSingleRow(cmd, headers, row, rec, "no metrics found")
}

func runMetricsAll(cmd *cobra.Command) error {
	conn, _, err := connect(cmd)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Close(ctx)

	rows, err := conn.Conn.Query(ctx, "SELECT queue_name, queue_length, newest_msg_age_sec, oldest_msg_age_sec FROM pgmq.metrics_all();")
	if err != nil {
		return dbError(err)
	}
	defer rows.Close()

	var records []metricsRecord
	for rows.Next() {
		var newestMsgAgeSec sql.NullInt64
		var oldestMsgAgeSec sql.NullInt64
		var rec metricsRecord
		if err := rows.Scan(
			&rec.QueueName,
			&rec.QueueLength,
			&newestMsgAgeSec,
			&oldestMsgAgeSec,
		); err != nil {
			return dbError(err)
		}
		rec.NewestMsgAgeSec = nullInt64Ptr(newestMsgAgeSec)
		rec.OldestMsgAgeSec = nullInt64Ptr(oldestMsgAgeSec)
		records = append(records, rec)
	}
	if rows.Err() != nil {
		return dbError(rows.Err())
	}
	headers := []string{"queue_name", "queue_length", "newest_msg_age_sec", "oldest_msg_age_sec"}
	tableRows := make([][]string, 0, len(records))
	for _, rec := range records {
		tableRows = append(tableRows, []string{
			rec.QueueName,
			int64String(rec.QueueLength),
			optionalInt64String(rec.NewestMsgAgeSec),
			optionalInt64String(rec.OldestMsgAgeSec),
		})
	}
	return renderOutput(cmd, headers, tableRows, records, "no queues found")
}

func nullInt64Ptr(v sql.NullInt64) *int64 {
	if !v.Valid {
		return nil
	}
	out := v.Int64
	return &out
}

func optionalInt64String(v *int64) string {
	if v == nil {
		return ""
	}
	return int64String(*v)
}
