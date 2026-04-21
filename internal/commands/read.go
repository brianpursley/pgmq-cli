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
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"pgmq-cli/internal/errs"
)

type readRecord struct {
	MsgID      int64           `json:"msg_id"`
	ReadCt     int64           `json:"read_ct"`
	EnqueuedAt time.Time       `json:"enqueued_at"`
	Vt         time.Time       `json:"vt"`
	Message    json.RawMessage `json:"message"`
	Headers    json.RawMessage `json:"headers"`
}

const (
	readStrategyStandard    = "standard"
	readStrategyGrouped     = "grouped"
	readStrategyGroupedRR   = "grouped-rr"
	readStrategyGroupedHead = "grouped-head"
)

var readStrategies = []string{
	readStrategyStandard,
	readStrategyGrouped,
	readStrategyGroupedRR,
	readStrategyGroupedHead,
}

func ReadCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "read <queue>",
		Short: "Read messages",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRead(cmd, args[0])
		},
	}
	cmd.Flags().Int("vt", 30, "Visibility timeout in seconds")
	cmd.Flags().Int("qty", 1, "Quantity to read")
	cmd.Flags().String("strategy", readStrategyStandard, "Read strategy: standard, grouped, grouped-rr, or grouped-head")
	_ = cmd.RegisterFlagCompletionFunc("strategy", readStrategyCompletion)
	addOutputFlag(cmd, false)
	cmd.ValidArgsFunction = queueNameCompletion
	return cmd
}

func runRead(cmd *cobra.Command, queue string) error {
	vt, err := cmd.Flags().GetInt("vt")
	if err != nil {
		return errs.NewUsageError("failed to read --vt flag")
	}
	if vt < 0 {
		return errs.NewUsageError("--vt must be >= 0")
	}
	qty, err := cmd.Flags().GetInt("qty")
	if err != nil {
		return errs.NewUsageError("failed to read --qty flag")
	}
	if qty <= 0 {
		return errs.NewUsageError("--qty must be >= 1")
	}
	strategy, err := cmd.Flags().GetString("strategy")
	if err != nil {
		return errs.NewUsageError("failed to read --strategy flag")
	}
	readQuery, err := readQueryForStrategy(strategy)
	if err != nil {
		return err
	}

	conn, _, err := connect(cmd)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Close(ctx)

	rows, err := queryRows(ctx, conn, readQuery, queue, vt, qty)
	if err != nil {
		if strategy != readStrategyStandard {
			return dbErrorForFIFOQueue(err, queue)
		}
		return dbErrorForQueue(err, queue)
	}
	defer rows.Close()

	var records []readRecord
	for rows.Next() {
		var rec readRecord
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
	if format == "json" {
		return outputJSONByQty(cmd, qty, records)
	}

	headers := []string{"msg_id", "read_ct", "enqueued_at", "vt", "message", "headers"}
	tableRows := make([][]string, 0, len(records))
	for _, rec := range records {
		tableRows = append(tableRows, readRecordRow(rec))
	}
	return renderOutput(cmd, headers, tableRows, records, "no messages found")
}

func readQueryForStrategy(strategy string) (string, error) {
	var functionName string
	switch strategy {
	case readStrategyStandard:
		functionName = "read"
	case readStrategyGrouped:
		functionName = "read_grouped"
	case readStrategyGroupedRR:
		functionName = "read_grouped_rr"
	case readStrategyGroupedHead:
		functionName = "read_grouped_head"
	default:
		return "", errs.NewUsageError(fmt.Sprintf("invalid --strategy %q (expected one of: %s)", strategy, strings.Join(readStrategies, ", ")))
	}
	return fmt.Sprintf("SELECT msg_id, read_ct, enqueued_at, vt, message, headers FROM pgmq.%s($1::text, $2, $3);", functionName), nil
}

func readStrategyCompletion(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	out := make([]string, 0, len(readStrategies))
	for _, strategy := range readStrategies {
		if toComplete == "" || strings.HasPrefix(strategy, toComplete) {
			out = append(out, strategy)
		}
	}
	return out, cobra.ShellCompDirectiveNoFileComp
}
