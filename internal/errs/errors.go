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

package errs

import "errors"

const (
	ExitSuccess  = 0
	ExitError    = 1
	ExitUsage    = 2
	ExitNotFound = 3
)

// CLIError maps an error to a specific exit code.
type CLIError struct {
	Code    int
	Message string
}

func (e CLIError) Error() string {
	return e.Message
}

func NewError(code int, message string) error {
	return CLIError{Code: code, Message: message}
}

func NewUsageError(message string) error {
	return CLIError{Code: ExitUsage, Message: message}
}

func NewNotFoundError(message string) error {
	return CLIError{Code: ExitNotFound, Message: message}
}

// ExitCodeFromError maps errors to exit codes.
func ExitCodeFromError(err error) int {
	if err == nil {
		return ExitSuccess
	}
	var cliErr CLIError
	if errors.As(err, &cliErr) {
		return cliErr.Code
	}
	return ExitError
}
