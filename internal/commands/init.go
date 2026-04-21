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
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"

	"pgmq-cli/internal/errs"
	"pgmq-cli/internal/output"
)

func ExtensionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "extension",
		Short: "Manage the PGMQ extension",
	}

	cmd.AddCommand(
		ExtensionInitCmd(),
		ExtensionStatusCmd(),
		ExtensionVersionCmd(),
	)

	return cmd
}

func ExtensionInitCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "Initialize pgmq extension",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runExtensionInit(cmd)
		},
	}
}

type extensionStatusRecord struct {
	Initialized bool    `json:"initialized"`
	Version     *string `json:"version"`
}

func ExtensionStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show pgmq extension status",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runExtensionStatus(cmd)
		},
	}
	addOutputFlag(cmd, false)
	return cmd
}

func ExtensionVersionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print the installed pgmq extension version",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runExtensionVersion(cmd)
		},
	}
	addOutputFlag(cmd, false)
	return cmd
}

func InitCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "init",
		Short:  "Initialize pgmq extension",
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			checkOnly, err := cmd.Flags().GetBool("check")
			if err != nil {
				return errs.NewUsageError("failed to read --check flag")
			}
			if checkOnly {
				fmt.Fprintln(cmd.ErrOrStderr(), "pgmq init --check is deprecated; use pgmq extension status instead")
				return runExtensionCheck(cmd)
			}
			fmt.Fprintln(cmd.ErrOrStderr(), "pgmq init is deprecated; use pgmq extension init instead")
			return runExtensionInit(cmd)
		},
	}
	cmd.Flags().Bool("check", false, "Check only")
	return cmd
}

func runExtensionInit(cmd *cobra.Command) error {
	conn, _, err := connect(cmd)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Close(ctx)

	version, ok, err := pgmqExtensionVersion(ctx, conn.Conn)
	if err != nil {
		return dbError(err)
	}
	if ok {
		return outputString(cmd, fmt.Sprintf("extension already initialized (version %s)", version))
	}

	_, err = conn.Conn.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;")
	if err != nil {
		return dbError(err)
	}
	version, ok, err = pgmqExtensionVersion(ctx, conn.Conn)
	if err != nil {
		return dbError(err)
	}
	if !ok {
		return errs.NewError(errs.ExitError, "extension initialized but version lookup failed")
	}
	return outputString(cmd, fmt.Sprintf("extension initialized (version %s)", version))
}

func runExtensionCheck(cmd *cobra.Command) error {
	conn, _, err := connect(cmd)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Close(ctx)

	version, ok, err := pgmqExtensionVersion(ctx, conn.Conn)
	if err != nil {
		return dbError(err)
	}
	if ok {
		return outputString(cmd, fmt.Sprintf("extension is initialized (version %s)", version))
	}
	return errs.NewError(errs.ExitError, "extension not initialized")
}

func runExtensionStatus(cmd *cobra.Command) error {
	conn, _, err := connect(cmd)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Close(ctx)

	version, ok, err := pgmqExtensionVersion(ctx, conn.Conn)
	if err != nil {
		return dbError(err)
	}

	return renderExtensionStatus(cmd, newExtensionStatusRecord(version, ok))
}

func runExtensionVersion(cmd *cobra.Command) error {
	conn, _, err := connect(cmd)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Close(ctx)

	version, ok, err := pgmqExtensionVersion(ctx, conn.Conn)
	if err != nil {
		return dbError(err)
	}
	if !ok {
		return errs.NewError(errs.ExitError, "extension not initialized")
	}

	return renderExtensionVersion(cmd, version)
}

func newExtensionStatusRecord(version string, initialized bool) extensionStatusRecord {
	if !initialized {
		return extensionStatusRecord{Initialized: false}
	}
	return extensionStatusRecord{Initialized: true, Version: &version}
}

func renderExtensionStatus(cmd *cobra.Command, record extensionStatusRecord) error {
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}
	if format == "json" {
		return output.PrintJSON(cmd.OutOrStdout(), record)
	}

	version := ""
	if record.Version != nil {
		version = *record.Version
	}
	headers := []string{"initialized", "version"}
	row := []string{boolString(record.Initialized), version}
	return renderSingleRow(cmd, headers, row, record, "extension status unavailable")
}

func renderExtensionVersion(cmd *cobra.Command, version string) error {
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}
	if format == "json" {
		return output.PrintJSON(cmd.OutOrStdout(), version)
	}
	fmt.Fprintln(cmd.OutOrStdout(), version)
	return nil
}

func pgmqExtensionVersion(ctx context.Context, conn *pgx.Conn) (string, bool, error) {
	var version string
	err := conn.QueryRow(ctx, "SELECT extversion FROM pg_extension WHERE extname = 'pgmq'").Scan(&version)
	if err == nil {
		return version, true, nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	return "", false, err
}
