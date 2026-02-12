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

func DropCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "drop <queue>",
		Short: "Drop a queue and its archive",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDrop(cmd, args[0])
		},
	}
	cmd.ValidArgsFunction = queueNameCompletion
	return cmd
}

func runDrop(cmd *cobra.Command, queue string) error {
	ok, err := confirmOrCancel(cmd, "Drop the queue and its archive?")
	if err != nil {
		return err
	}
	if !ok {
		return errs.NewError(errs.ExitError, "operation cancelled")
	}

	conn, _, err := connect(cmd)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Close(ctx)

	var dropped bool
	if err := conn.Conn.QueryRow(ctx, "SELECT pgmq.drop_queue($1::text);", queue).Scan(&dropped); err != nil {
		return dbErrorForQueue(err, queue)
	}
	if !dropped {
		return errs.NewNotFoundError("queue not found")
	}
	return outputString(cmd, "queue dropped")
}
