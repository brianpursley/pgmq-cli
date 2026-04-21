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
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	"pgmq-cli/internal/config"
	"pgmq-cli/internal/db"
	"pgmq-cli/internal/errs"
	"pgmq-cli/internal/output"
)

type resolvedTarget struct {
	ServerName string
	ConnString string
}

type rowsScanner interface {
	Next() bool
	Scan(dest ...any) error
	Close()
	Err() error
}

func resolveTarget(cmd *cobra.Command) (resolvedTarget, error) {
	serverName, err := getStringFlag(cmd, "server")
	if err != nil {
		return resolvedTarget{}, errs.NewUsageError("failed to read --server flag")
	}

	cfg, err := loadConfigForRead(cmd)
	if err != nil {
		return resolvedTarget{}, err
	}

	name, entry, err := cfg.ResolveServer(serverName)
	if err != nil {
		switch {
		case errors.Is(err, config.ErrNoServer):
			return resolvedTarget{}, errs.NewUsageError("server required: set defaultServer or pass --server")
		case errors.Is(err, config.ErrServerNotFound):
			available := strings.Join(cfg.ServerNames(), ", ")
			if available == "" {
				available = "(none configured)"
			}
			return resolvedTarget{}, errs.NewUsageError(fmt.Sprintf("server %q not found. available: %s", serverName, available))
		default:
			return resolvedTarget{}, errs.NewError(errs.ExitError, err.Error())
		}
	}

	if entry.ConnectionString == "" {
		return resolvedTarget{}, errs.NewUsageError(fmt.Sprintf("server %q has an empty connectionString", name))
	}

	return resolvedTarget{ServerName: name, ConnString: entry.ConnectionString}, nil
}

func connect(cmd *cobra.Command) (*db.DB, resolvedTarget, error) {
	target, err := resolveTarget(cmd)
	if err != nil {
		return nil, resolvedTarget{}, err
	}
	ctx := context.Background()
	conn, err := db.Connect(ctx, target.ConnString)
	if err != nil {
		return nil, resolvedTarget{}, dbError(err)
	}
	return conn, target, nil
}

func loadConfigForRead(cmd *cobra.Command) (*config.Config, error) {
	path, err := configPath(cmd)
	if err != nil {
		return nil, err
	}
	cfg, err := config.LoadFromPath(path)
	if err != nil {
		if errors.Is(err, config.ErrConfigNotFound) {
			return nil, errs.NewUsageError(fmt.Sprintf("config not found at %s", path))
		}
		return nil, errs.NewError(errs.ExitError, err.Error())
	}
	return cfg, nil
}

func configPath(cmd *cobra.Command) (string, error) {
	path, err := getStringFlag(cmd, "config")
	if err != nil {
		return "", errs.NewUsageError("failed to read --config flag")
	}
	if path == "" {
		return config.DefaultPath()
	}
	return path, nil
}

func queryRows(ctx context.Context, conn *db.DB, sql string, args ...any) (rowsScanner, error) {
	return conn.Conn.Query(ctx, sql, args...)
}

func dbError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42883":
			return errs.NewError(errs.ExitError, "pgmq functions not found: is the extension installed? try `pgmq init`")
		case "42P01", "42704":
			return errs.NewNotFoundError("resource not found")
		}
	}
	return errs.NewError(errs.ExitError, err.Error())
}

func dbErrorForQueue(err error, queue string) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42883":
			return errs.NewError(errs.ExitError, "pgmq functions not found: is the extension installed? try `pgmq init`")
		case "42P01", "42704":
			return errs.NewNotFoundError(fmt.Sprintf("queue %q not found", queue))
		}
		if strings.Contains(strings.ToLower(pgErr.Message), "does not exist") &&
			strings.Contains(strings.ToLower(pgErr.Message), "queue") {
			return errs.NewNotFoundError(fmt.Sprintf("queue %q not found", queue))
		}
	}
	return errs.NewError(errs.ExitError, err.Error())
}

func dbErrorForTopic(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42883" {
		return errs.NewError(errs.ExitError, "topic routing functions not found; ensure the pgmq extension is installed and upgraded to 1.11.0 or later")
	}
	return dbError(err)
}

func dbErrorForTopicQueue(err error, queue string) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42883" {
		return errs.NewError(errs.ExitError, "topic routing functions not found; ensure the pgmq extension is installed and upgraded to 1.11.0 or later")
	}
	return dbErrorForQueue(err, queue)
}

func dbErrorForFIFO(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42883" {
		return errs.NewError(errs.ExitError, "FIFO functions not found; ensure the pgmq extension is installed and upgraded to 1.11.1 or later")
	}
	return dbError(err)
}

func dbErrorForFIFOQueue(err error, queue string) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42883" {
		return errs.NewError(errs.ExitError, "FIFO functions not found; ensure the pgmq extension is installed and upgraded to 1.11.1 or later")
	}
	return dbErrorForQueue(err, queue)
}

func outputString(cmd *cobra.Command, message string) error {
	format, err := getStringFlag(cmd, "output")
	if err == nil && format == "json" {
		return output.PrintJSON(cmd.OutOrStdout(), message)
	}
	fmt.Fprintln(cmd.OutOrStdout(), message)
	return nil
}

func addOutputFlag(cmd *cobra.Command, allowMessage bool) {
	if allowMessage {
		cmd.Flags().StringP("output", "o", "table", "Output format: table, json, or message")
		_ = cmd.RegisterFlagCompletionFunc("output", outputCompletion(true))
		return
	}
	cmd.Flags().StringP("output", "o", "table", "Output format: table or json")
	_ = cmd.RegisterFlagCompletionFunc("output", outputCompletion(false))
}

func outputCompletion(allowMessage bool) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		values := []string{"table", "json"}
		if allowMessage {
			values = append(values, "message")
		}
		out := make([]string, 0, len(values))
		for _, v := range values {
			if toComplete == "" || strings.HasPrefix(v, toComplete) {
				out = append(out, v)
			}
		}
		return out, cobra.ShellCompDirectiveNoFileComp
	}
}

func renderOutput(cmd *cobra.Command, headers []string, rows [][]string, jsonData any, emptyMessage string) error {
	if len(rows) == 0 {
		return outputEmpty(cmd, emptyMessage)
	}
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}
	if format == "json" {
		return output.PrintJSON(cmd.OutOrStdout(), jsonData)
	}
	output.PrintTable(cmd.OutOrStdout(), headers, rows)
	return nil
}

func renderSingleRow(cmd *cobra.Command, headers []string, row []string, jsonData any, emptyMessage string) error {
	if len(row) == 0 {
		return outputEmpty(cmd, emptyMessage)
	}
	return renderOutput(cmd, headers, [][]string{row}, jsonData, emptyMessage)
}

func readRecordRow(rec readRecord) []string {
	return []string{
		int64String(rec.MsgID),
		int64String(rec.ReadCt),
		rec.EnqueuedAt.Format(time.RFC3339),
		rec.Vt.Format(time.RFC3339),
		string(rec.Message),
		string(rec.Headers),
	}
}

func popRecordRow(rec popRecord) []string {
	return []string{
		int64String(rec.MsgID),
		int64String(rec.ReadCt),
		rec.EnqueuedAt.Format(time.RFC3339),
		rec.Vt.Format(time.RFC3339),
		string(rec.Message),
		string(rec.Headers),
	}
}

func confirmOrCancel(cmd *cobra.Command, prompt string) (bool, error) {
	yes, err := getBoolFlag(cmd, "yes")
	if err != nil {
		return false, errs.NewUsageError("failed to read --yes flag")
	}
	if yes {
		return true, nil
	}
	return promptYesNo(cmd, prompt)
}

func promptYesNo(cmd *cobra.Command, prompt string) (bool, error) {
	if !isTerminal(cmd.InOrStdin()) {
		return false, nil
	}
	fmt.Fprintf(cmd.ErrOrStderr(), "%s [y/N]: ", prompt)
	var response string
	_, err := fmt.Fscanln(cmd.InOrStdin(), &response)
	if err != nil {
		return false, errs.NewError(errs.ExitError, "failed to read confirmation")
	}
	switch strings.ToLower(strings.TrimSpace(response)) {
	case "y", "yes":
		return true, nil
	default:
		return false, nil
	}
}

func boolString(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

func int64String(v int64) string {
	return fmt.Sprintf("%d", v)
}

func headersOrEmpty(headers json.RawMessage) json.RawMessage {
	if len(headers) == 0 || string(headers) == "null" {
		return json.RawMessage("{}")
	}
	return headers
}

func isTerminal(r io.Reader) bool {
	if f, ok := r.(*os.File); ok {
		return term.IsTerminal(int(f.Fd()))
	}
	return false
}

func parseIDs(ids []string) ([]int64, error) {
	out := make([]int64, 0, len(ids))
	for _, id := range ids {
		parsed, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			return nil, errs.NewUsageError("msg_id must be an integer")
		}
		out = append(out, parsed)
	}
	return out, nil
}

func outputSingleID(cmd *cobra.Command, id int64) error {
	return outputIDs(cmd, []int64{id})
}

func outputIDs(cmd *cobra.Command, ids []int64) error {
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}
	if format == "json" {
		return output.PrintJSON(cmd.OutOrStdout(), ids)
	}
	headers := []string{"msg_id"}
	rows := make([][]string, 0, len(ids))
	for _, id := range ids {
		rows = append(rows, []string{strconv.FormatInt(id, 10)})
	}
	output.PrintTable(cmd.OutOrStdout(), headers, rows)
	return nil
}

func outputJSONByQty(cmd *cobra.Command, qty int, records any) error {
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}
	if format != "json" {
		return errs.NewUsageError("outputJSONByQty is only valid for json output")
	}
	if qty <= 1 {
		switch v := records.(type) {
		case []readRecord:
			if len(v) == 0 {
				return nil
			}
			return output.PrintJSON(cmd.OutOrStdout(), v[0])
		case []popRecord:
			if len(v) == 0 {
				return nil
			}
			return output.PrintJSON(cmd.OutOrStdout(), v[0])
		default:
			return output.PrintJSON(cmd.OutOrStdout(), records)
		}
	}
	return output.PrintJSON(cmd.OutOrStdout(), records)
}

func outputEmpty(cmd *cobra.Command, message string) error {
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}
	if format == "json" {
		// Always emit valid JSON in machine-readable mode.
		return output.PrintJSON(cmd.OutOrStdout(), []any{})
	}
	return outputString(cmd, message)
}

func outputEmptyByQty(cmd *cobra.Command, qty int, message string) error {
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}
	if format == "json" {
		if qty <= 1 {
			return output.PrintJSON(cmd.OutOrStdout(), nil)
		}
		return output.PrintJSON(cmd.OutOrStdout(), []any{})
	}
	return outputString(cmd, message)
}

func outputMessages(cmd *cobra.Command, qty int, messages []json.RawMessage) error {
	if qty <= 1 {
		if len(messages) == 0 {
			return nil
		}
		return output.PrintJSON(cmd.OutOrStdout(), messages[0])
	}
	return output.PrintJSON(cmd.OutOrStdout(), messages)
}

func getOutputFormat(cmd *cobra.Command) (string, error) {
	format, err := getStringFlag(cmd, "output")
	if err != nil {
		return "", errs.NewUsageError("failed to read --output flag")
	}
	switch format {
	case "table", "json":
		return format, nil
	case "message":
		if cmd.Name() != "pop" {
			return "", errs.NewUsageError("--output message is only valid for pop")
		}
		return format, nil
	default:
		return "", errs.NewUsageError("invalid output format")
	}
}

func getStringFlag(cmd *cobra.Command, name string) (string, error) {
	if cmd.Flags().Lookup(name) != nil {
		return cmd.Flags().GetString(name)
	}
	if cmd.InheritedFlags().Lookup(name) != nil {
		return cmd.InheritedFlags().GetString(name)
	}
	return "", fmt.Errorf("flag not found: %s", name)
}

func getBoolFlag(cmd *cobra.Command, name string) (bool, error) {
	if cmd.Flags().Lookup(name) != nil {
		return cmd.Flags().GetBool(name)
	}
	if cmd.InheritedFlags().Lookup(name) != nil {
		return cmd.InheritedFlags().GetBool(name)
	}
	return false, fmt.Errorf("flag not found: %s", name)
}

func queueNameCompletion(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) > 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return completeQueueNames(cmd, toComplete)
}

func completeQueueNames(cmd *cobra.Command, toComplete string) ([]string, cobra.ShellCompDirective) {
	conn, _, err := connect(cmd)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	ctx := context.Background()
	defer conn.Close(ctx)

	rows, err := conn.Conn.Query(ctx, "SELECT queue_name FROM pgmq.list_queues();")
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	defer rows.Close()

	out := make([]string, 0)
	for rows.Next() {
		var queueName string
		if err := rows.Scan(&queueName); err != nil {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		if toComplete == "" || strings.HasPrefix(queueName, toComplete) {
			out = append(out, queueName)
		}
	}
	if rows.Err() != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return out, cobra.ShellCompDirectiveNoFileComp
}
