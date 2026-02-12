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
	"strings"

	"github.com/spf13/cobra"

	"pgmq-cli/internal/errs"
)

func CreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create <queue>",
		Short: "Create a queue",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCreate(cmd, args[0])
		},
	}
	cmd.Flags().Bool("logged", true, "Create logged queue")
	_ = cmd.RegisterFlagCompletionFunc("logged", loggedFlagCompletion)
	return cmd
}

func loggedFlagCompletion(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	values := []string{"true", "false"}
	out := make([]string, 0, len(values))
	for _, v := range values {
		if toComplete == "" || strings.HasPrefix(v, toComplete) {
			out = append(out, v)
		}
	}
	return out, cobra.ShellCompDirectiveNoFileComp
}

func runCreate(cmd *cobra.Command, queue string) error {
	logged, err := cmd.Flags().GetBool("logged")
	if err != nil {
		return errs.NewUsageError("failed to read --logged flag")
	}

	conn, _, err := connect(cmd)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Close(ctx)

	sql := "SELECT pgmq.create($1);"
	if !logged {
		sql = "SELECT pgmq.create_unlogged($1);"
	}

	_, err = conn.Conn.Exec(ctx, sql, queue)
	if err != nil {
		return dbError(err)
	}

	return outputString(cmd, "queue created")
}
