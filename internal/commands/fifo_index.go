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

	"pgmq-cli/internal/errs"
)

func FIFOIndexCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "index [queue]",
		Short: "Create FIFO indexes",
		Args:  validateFIFOIndexArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			all, err := cmd.Flags().GetBool("all")
			if err != nil {
				return errs.NewUsageError("failed to read --all flag")
			}
			queue := ""
			if len(args) == 1 {
				queue = args[0]
			}
			return runFIFOIndex(cmd, queue, all)
		},
	}
	cmd.Flags().Bool("all", false, "Create FIFO indexes on all queues")
	cmd.ValidArgsFunction = fifoIndexQueueCompletion
	return cmd
}

func validateFIFOIndexArgs(cmd *cobra.Command, args []string) error {
	all, err := cmd.Flags().GetBool("all")
	if err != nil {
		return errs.NewUsageError("failed to read --all flag")
	}
	if all && len(args) > 0 {
		return errs.NewUsageError("--all cannot be used with a queue")
	}
	if !all && len(args) == 0 {
		return errs.NewUsageError("queue required unless --all is set")
	}
	if !all && len(args) > 1 {
		return errs.NewUsageError("accepts at most one queue argument")
	}
	return nil
}

func runFIFOIndex(cmd *cobra.Command, queue string, all bool) error {
	conn, _, err := connect(cmd)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Close(ctx)

	if all {
		if _, err := conn.Conn.Exec(ctx, "SELECT pgmq.create_fifo_indexes_all();"); err != nil {
			return dbErrorForFIFO(err)
		}
		return outputString(cmd, "fifo indexes created")
	}

	if _, err := conn.Conn.Exec(ctx, "SELECT pgmq.create_fifo_index($1::text);", queue); err != nil {
		return dbErrorForFIFOQueue(err, queue)
	}
	return outputString(cmd, "fifo index created")
}

func fifoIndexQueueCompletion(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	all, err := cmd.Flags().GetBool("all")
	if err == nil && all {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return queueNameCompletion(cmd, args, toComplete)
}
