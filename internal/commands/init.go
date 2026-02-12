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
)

func InitCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize pgmq extension",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runInit(cmd)
		},
	}
	cmd.Flags().Bool("check", false, "Check only")
	return cmd
}

func runInit(cmd *cobra.Command) error {
	conn, _, err := connect(cmd)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Close(ctx)

	checkOnly, err := cmd.Flags().GetBool("check")
	if err != nil {
		return errs.NewUsageError("failed to read --check flag")
	}

	if checkOnly {
		version, ok, err := pgmqExtensionVersion(ctx, conn.Conn)
		if err != nil {
			return dbError(err)
		}
		if ok {
			return outputString(cmd, fmt.Sprintf("extension is initialized (version %s)", version))
		}
		return errs.NewError(errs.ExitError, "extension not initialized")
	}

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
