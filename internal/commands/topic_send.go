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

import "github.com/spf13/cobra"

func TopicSendCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "send <routing-key> <message-json>",
		Short: "Send a message using topic routing",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTopicSend(cmd, args[0], args[1])
		},
	}
	cmd.Flags().String("headers", "", "JSON headers")
	cmd.Flags().Int("delay", 0, "Delay in seconds")
	cmd.Flags().String("delay-until", "", "Delay until RFC3339 timestamp")
	addOutputFlag(cmd, false)
	return cmd
}

func runTopicSend(cmd *cobra.Command, routingKey, message string) error {
	return topicCommandNotImplemented("topic send")
}
