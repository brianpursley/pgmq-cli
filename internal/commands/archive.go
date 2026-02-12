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

func ArchiveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "archive <queue> <msg_id> [msg_id...]",
		Short: "Archive messages",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runArchive(cmd, args[0], args[1:])
		},
	}
	addOutputFlag(cmd, false)
	cmd.ValidArgsFunction = queueNameCompletion
	return cmd
}

func runArchive(cmd *cobra.Command, queue string, ids []string) error {
	msgIDs, err := parseIDs(ids)
	if err != nil {
		return err
	}

	conn, _, err := connect(cmd)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Close(ctx)

	if len(msgIDs) == 1 {
		var archived bool
		err = conn.Conn.QueryRow(ctx, "SELECT pgmq.archive($1::text, $2::bigint);", queue, msgIDs[0]).Scan(&archived)
		if err != nil {
			return dbErrorForQueue(err, queue)
		}
		if !archived {
			return errs.NewNotFoundError("message not found")
		}
		return outputSingleID(cmd, msgIDs[0])
	}

	rows, err := conn.Conn.Query(ctx, "SELECT pgmq.archive($1::text, $2::bigint[]) AS msg_id;", queue, msgIDs)
	if err != nil {
		return dbErrorForQueue(err, queue)
	}
	defer rows.Close()

	var archivedIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return dbError(err)
		}
		archivedIDs = append(archivedIDs, id)
	}
	if rows.Err() != nil {
		return dbErrorForQueue(rows.Err(), queue)
	}
	if len(archivedIDs) == 0 {
		return errs.NewNotFoundError("no messages found to archive")
	}
	return outputIDs(cmd, archivedIDs)
}
