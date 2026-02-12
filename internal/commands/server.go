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
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"pgmq-cli/internal/config"
	"pgmq-cli/internal/errs"
	"pgmq-cli/internal/output"
)

func ServerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "server",
		Short: "Manage configured servers",
	}

	cmd.AddCommand(
		ServerAddCmd(),
		ServerUpdateCmd(),
		ServerRemoveCmd(),
		ServerListCmd(),
		ServerGetCmd(),
		ServerGetDefaultCmd(),
		ServerSetDefaultCmd(),
		ServerUnsetDefaultCmd(),
	)

	return cmd
}

func ServerAddCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "add <name> <connectionString>",
		Short: "Add a server to config",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServerAdd(cmd, args[0], args[1])
		},
	}
}

func ServerUpdateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update <name> <connectionString>",
		Short: "Update a server in config",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServerUpdate(cmd, args[0], args[1])
		},
	}
	cmd.ValidArgsFunction = serverNameCompletion
	return cmd
}

func ServerRemoveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remove <name>",
		Short: "Remove a server from config",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServerRemove(cmd, args[0])
		},
	}
	cmd.ValidArgsFunction = serverNameCompletion
	return cmd
}

func ServerListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List configured servers",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServerList(cmd)
		},
	}
	addOutputFlag(cmd, false)
	return cmd
}

func ServerGetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <name>",
		Short: "Show a configured server",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServerGet(cmd, args[0])
		},
	}
	addOutputFlag(cmd, false)
	cmd.ValidArgsFunction = serverNameCompletion
	return cmd
}

func ServerSetDefaultCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-default <name>",
		Short: "Set default server",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServerSetDefault(cmd, args[0])
		},
	}
	cmd.ValidArgsFunction = serverNameCompletion
	return cmd
}

func ServerGetDefaultCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get-default",
		Short: "Show default server",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServerGetDefault(cmd)
		},
	}
	addOutputFlag(cmd, false)
	return cmd
}

func ServerUnsetDefaultCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "unset-default",
		Short: "Unset default server",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServerUnsetDefault(cmd)
		},
	}
}

func runServerAdd(cmd *cobra.Command, name, conn string) error {
	cfg, path, err := loadOrInitConfig(cmd)
	if err != nil {
		return err
	}
	if _, exists := cfg.Servers[name]; exists {
		return errs.NewUsageError(fmt.Sprintf("server %q already exists", name))
	}
	cfg.Servers[name] = config.ServerEntry{ConnectionString: conn}
	return saveConfig(cmd, path, cfg, fmt.Sprintf("server %q added", name))
}

func runServerUpdate(cmd *cobra.Command, name, conn string) error {
	cfg, path, err := loadOrInitConfig(cmd)
	if err != nil {
		return err
	}
	if _, exists := cfg.Servers[name]; !exists {
		return errs.NewNotFoundError(fmt.Sprintf("server %q not found", name))
	}
	cfg.Servers[name] = config.ServerEntry{ConnectionString: conn}
	return saveConfig(cmd, path, cfg, fmt.Sprintf("server %q updated", name))
}

func runServerRemove(cmd *cobra.Command, name string) error {
	cfg, path, err := loadOrInitConfig(cmd)
	if err != nil {
		return err
	}
	if _, exists := cfg.Servers[name]; !exists {
		return errs.NewNotFoundError(fmt.Sprintf("server %q not found", name))
	}
	delete(cfg.Servers, name)
	if cfg.DefaultServer == name {
		cfg.DefaultServer = ""
	}
	return saveConfig(cmd, path, cfg, fmt.Sprintf("server %q removed", name))
}

func runServerList(cmd *cobra.Command) error {
	cfg, err := loadConfigForRead(cmd)
	if err != nil {
		return err
	}

	type serverRow struct {
		Name    string `json:"name"`
		Default bool   `json:"default"`
	}

	rows := make([]serverRow, 0, len(cfg.Servers))
	for name := range cfg.Servers {
		rows = append(rows, serverRow{
			Name:    name,
			Default: cfg.DefaultServer == name,
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })

	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}
	if format == "json" {
		return output.PrintJSON(cmd.OutOrStdout(), rows)
	}

	headers := []string{"name", "default"}
	tableRows := make([][]string, 0, len(rows))
	for _, row := range rows {
		tableRows = append(tableRows, []string{
			row.Name,
			boolString(row.Default),
		})
	}
	output.PrintTable(cmd.OutOrStdout(), headers, tableRows)
	return nil
}

func runServerGet(cmd *cobra.Command, name string) error {
	cfg, err := loadConfigForRead(cmd)
	if err != nil {
		return err
	}
	entry, ok := cfg.Servers[name]
	if !ok {
		return errs.NewNotFoundError(fmt.Sprintf("server %q not found", name))
	}

	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}
	if format == "json" {
		return output.PrintJSON(cmd.OutOrStdout(), map[string]any{
			"name":             name,
			"connectionString": entry.ConnectionString,
			"default":          cfg.DefaultServer == name,
		})
	}

	headers := []string{"name", "connectionString", "default"}
	rows := [][]string{{
		name,
		entry.ConnectionString,
		boolString(cfg.DefaultServer == name),
	}}
	output.PrintTable(cmd.OutOrStdout(), headers, rows)
	return nil
}

func runServerSetDefault(cmd *cobra.Command, name string) error {
	cfg, path, err := loadOrInitConfig(cmd)
	if err != nil {
		return err
	}
	if _, exists := cfg.Servers[name]; !exists {
		return errs.NewNotFoundError(fmt.Sprintf("server %q not found", name))
	}
	cfg.DefaultServer = name
	return saveConfig(cmd, path, cfg, fmt.Sprintf("default server set to %q", name))
}

func runServerGetDefault(cmd *cobra.Command) error {
	cfg, err := loadConfigForRead(cmd)
	if err != nil {
		return err
	}
	if cfg.DefaultServer == "" {
		return errs.NewNotFoundError("default server is not set")
	}

	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}
	if format == "json" {
		return output.PrintJSON(cmd.OutOrStdout(), map[string]string{
			"defaultServer": cfg.DefaultServer,
		})
	}
	return outputString(cmd, cfg.DefaultServer)
}

func runServerUnsetDefault(cmd *cobra.Command) error {
	cfg, path, err := loadOrInitConfig(cmd)
	if err != nil {
		return err
	}
	cfg.DefaultServer = ""
	return saveConfig(cmd, path, cfg, "default server unset")
}

func loadOrInitConfig(cmd *cobra.Command) (*config.Config, string, error) {
	path, err := configPath(cmd)
	if err != nil {
		return nil, "", errs.NewError(errs.ExitError, err.Error())
	}
	cfg, err := config.LoadOrInit(path)
	if err != nil {
		return nil, "", errs.NewError(errs.ExitError, err.Error())
	}
	return cfg, path, nil
}

func saveConfig(cmd *cobra.Command, path string, cfg *config.Config, message string) error {
	if err := config.SaveToPath(path, cfg); err != nil {
		return errs.NewError(errs.ExitError, err.Error())
	}
	return outputString(cmd, message)
}

func serverNameCompletion(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) > 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	cfg, _, err := loadOrInitConfig(cmd)
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
