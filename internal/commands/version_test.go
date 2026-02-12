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
	"strings"
	"testing"

	buildversion "pgmq-cli/internal/version"
)

func TestVersionCmd(t *testing.T) {
	orig := buildversion.Version
	buildversion.Version = "v1.2.3"
	t.Cleanup(func() {
		buildversion.Version = orig
	})

	cmd := VersionCmd()
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("version command failed: %v", err)
	}
	if strings.TrimSpace(out.String()) != "v1.2.3" {
		t.Fatalf("unexpected version output: %q", out.String())
	}
}
