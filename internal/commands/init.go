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
	"fmt"

	"github.com/spf13/cobra"

	"pgmq-cli/internal/errs"
)

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
