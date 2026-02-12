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
	"bytes"
	"testing"

	"pgmq-cli/internal/errs"
)

func TestSendFlagValidation(t *testing.T) {
	cmd := SendCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	cmd.PersistentFlags().String("config", "", "Config file path")
	cmd.Flags().Set("delay", "5")
	cmd.Flags().Set("delay-until", "2025-01-01T12:00:00Z")
	err := runSend(cmd, "queue", `{"a":1}`)
	assertExitCode(t, err, errs.ExitUsage)

	cmd = SendCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	cmd.PersistentFlags().String("config", "", "Config file path")
	cmd.Flags().Set("delay-until", "bad-time")
	err = runSend(cmd, "queue", `{"a":1}`)
	assertExitCode(t, err, errs.ExitUsage)

	cmd = SendCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	cmd.PersistentFlags().String("config", "", "Config file path")
	err = runSend(cmd, "queue", `not-json`)
	assertExitCode(t, err, errs.ExitUsage)

	cmd = SendCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(&bytes.Buffer{})
	cmd.PersistentFlags().String("config", "", "Config file path")
	cmd.Flags().Set("delay", "-1")
	err = runSend(cmd, "queue", `{"a":1}`)
	assertExitCode(t, err, errs.ExitUsage)
}
