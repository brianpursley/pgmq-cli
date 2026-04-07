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

import "testing"

func TestTopicSubcommandsRegisterExpectedChildren(t *testing.T) {
	cmd := TopicCmd()

	for _, name := range []string{"bind", "unbind", "list", "test", "send"} {
		if _, _, err := cmd.Find([]string{name}); err != nil {
			t.Fatalf("expected topic subcommand %q to be registered: %v", name, err)
		}
	}
}

func TestTopicCommandsWireQueueCompletion(t *testing.T) {
	if TopicBindCmd().ValidArgsFunction == nil {
		t.Fatalf("expected topic bind to wire queue completion")
	}
	if TopicUnbindCmd().ValidArgsFunction == nil {
		t.Fatalf("expected topic unbind to wire queue completion")
	}
	if TopicListCmd().ValidArgsFunction == nil {
		t.Fatalf("expected topic list to wire queue completion")
	}
}
