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
	"fmt"

	"github.com/spf13/cobra"

	"pgmq-cli/internal/errs"
)

func PurgeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "purge <queue>",
		Short: "Purge all messages from a queue",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPurge(cmd, args[0])
		},
	}
	cmd.ValidArgsFunction = queueNameCompletion
	return cmd
}

func runPurge(cmd *cobra.Command, queue string) error {
	ok, err := confirmOrCancel(cmd, "Purge all messages in the queue?")
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

	var exists bool
	if err := conn.Conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pgmq.list_queues() WHERE queue_name = $1::text);", queue).Scan(&exists); err != nil {
		return dbErrorForQueue(err, queue)
	}
	if !exists {
		return errs.NewNotFoundError(fmt.Sprintf("queue %q not found", queue))
	}

	var purgedCount int64
	if err := conn.Conn.QueryRow(ctx, "SELECT pgmq.purge_queue($1::text);", queue).Scan(&purgedCount); err != nil {
		return dbErrorForQueue(err, queue)
	}
	if purgedCount == 0 {
		return outputString(cmd, "queue is empty")
	}
	return outputString(cmd, "queue purged")
}
