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

package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"pgmq-cli/internal/commands"
	"pgmq-cli/internal/config"
	"pgmq-cli/internal/errs"
)

// Execute is the main CLI entrypoint.
// It returns an exit code instead of calling os.Exit directly.
func Execute(args []string) int {
	root := NewRootCmd()
	root.SetArgs(args)

	if err := root.Execute(); err != nil {
		printError(err)
		return errs.ExitCodeFromError(err)
	}
	return errs.ExitSuccess
}

// NewRootCmd constructs the root cobra command.
func NewRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "pgmq",
		Short:         "CLI for managing PGMQ",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	cmd.PersistentFlags().StringP("server", "s", "", "Server name (overrides defaultServer)")
	cmd.PersistentFlags().BoolP("yes", "Y", false, "Automatic yes to prompts")
	cmd.PersistentFlags().String("config", "", "Config file path (overrides default)")
	_ = cmd.RegisterFlagCompletionFunc("server", serverFlagCompletion)
	_ = cmd.RegisterFlagCompletionFunc("config", configFlagCompletion)

	// v1.0 commands
	cmd.AddCommand(
		commands.ServerCmd(),
		commands.InitCmd(),
		commands.CreateCmd(),
		commands.DropCmd(),
		commands.ListCmd(),
		commands.MetricsCmd(),
		commands.SendCmd(),
		commands.ReadCmd(),
		commands.PopCmd(),
		commands.DeleteCmd(),
		commands.ArchiveCmd(),
		commands.PurgeCmd(),
		commands.VersionCmd(),
	)

	return cmd
}

func printError(err error) {
	if err == nil {
		return
	}
	fmt.Fprintln(os.Stderr, err.Error())
}

func serverFlagCompletion(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	path, err := configPathForCompletion(cmd)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	cfg, err := config.LoadFromPath(path)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	out := make([]string, 0, len(cfg.Servers))
	for _, name := range cfg.ServerNames() {
		if toComplete == "" || strings.HasPrefix(name, toComplete) {
			out = append(out, name)
		}
	}
	return out, cobra.ShellCompDirectiveNoFileComp
}

func configPathForCompletion(cmd *cobra.Command) (string, error) {
	if cmd.Flags().Lookup("config") != nil {
		path, err := cmd.Flags().GetString("config")
		if err != nil {
			return "", err
		}
		if path != "" {
			return path, nil
		}
	}
	if cmd.InheritedFlags().Lookup("config") != nil {
		path, err := cmd.InheritedFlags().GetString("config")
		if err != nil {
			return "", err
		}
		if path != "" {
			return path, nil
		}
	}
	return config.DefaultPath()
}

func configFlagCompletion(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	return []string{"json"}, cobra.ShellCompDirectiveFilterFileExt
}
