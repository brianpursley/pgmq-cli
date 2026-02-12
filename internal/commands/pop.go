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

type popRecord struct {
	MsgID      int64           `json:"msg_id"`
	ReadCt     int64           `json:"read_ct"`
	EnqueuedAt time.Time       `json:"enqueued_at"`
	Vt         time.Time       `json:"vt"`
	Message    json.RawMessage `json:"message"`
	Headers    json.RawMessage `json:"headers"`
}

func PopCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pop <queue>",
		Short: "Pop messages",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPop(cmd, args[0])
		},
	}
	cmd.Flags().Int("qty", 1, "Quantity to pop")
	addOutputFlag(cmd, true)
	cmd.ValidArgsFunction = queueNameCompletion
	return cmd
}

func runPop(cmd *cobra.Command, queue string) error {
	qty, err := cmd.Flags().GetInt("qty")
	if err != nil {
		return errs.NewUsageError("failed to read --qty flag")
	}
	if qty <= 0 {
		return errs.NewUsageError("--qty must be >= 1")
	}

	conn, _, err := connect(cmd)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Close(ctx)

	var rows rowsScanner
	if qty == 1 {
		rows, err = queryRows(ctx, conn, "SELECT msg_id, read_ct, enqueued_at, vt, message, headers FROM pgmq.pop($1);", queue)
	} else {
		rows, err = queryRows(ctx, conn, "SELECT msg_id, read_ct, enqueued_at, vt, message, headers FROM pgmq.pop($1, $2);", queue, qty)
	}
	if err != nil {
		return dbErrorForQueue(err, queue)
	}
	defer rows.Close()

	var records []popRecord
	for rows.Next() {
		var rec popRecord
		var headers json.RawMessage
		if err := rows.Scan(&rec.MsgID, &rec.ReadCt, &rec.EnqueuedAt, &rec.Vt, &rec.Message, &headers); err != nil {
			return dbError(err)
		}
		rec.Headers = headersOrEmpty(headers)
		records = append(records, rec)
	}
	if rows.Err() != nil {
		return dbErrorForQueue(rows.Err(), queue)
	}
	if len(records) == 0 {
		return outputEmptyByQty(cmd, qty, "no messages found")
	}

	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}
	if format == "message" {
		messages := make([]json.RawMessage, 0, len(records))
		for _, rec := range records {
			messages = append(messages, rec.Message)
		}
		return outputMessages(cmd, qty, messages)
	}
	if format == "json" {
		return outputJSONByQty(cmd, qty, records)
	}

	headers := []string{"msg_id", "read_ct", "enqueued_at", "vt", "message", "headers"}
	tableRows := make([][]string, 0, len(records))
	for _, rec := range records {
		tableRows = append(tableRows, popRecordRow(rec))
	}
	return renderOutput(cmd, headers, tableRows, records, "no messages found")
}
