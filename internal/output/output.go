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

package output

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// PrintJSON writes v as JSON to the provided writer.
func PrintJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// PrintTable renders a simple fixed-width table.
func PrintTable(w io.Writer, headers []string, rows [][]string) {
	if len(headers) == 0 {
		return
	}

	colWidths := make([]int, len(headers))
	for i, h := range headers {
		colWidths[i] = len(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i >= len(colWidths) {
				continue
			}
			if len(cell) > colWidths[i] {
				colWidths[i] = len(cell)
			}
		}
	}

	writeRow := func(cells []string) {
		for i, cell := range cells {
			if i >= len(colWidths) {
				continue
			}
			padding := colWidths[i] - len(cell)
			fmt.Fprint(w, cell)
			if i < len(colWidths)-1 {
				fmt.Fprint(w, strings.Repeat(" ", padding+2))
			}
		}
		fmt.Fprintln(w)
	}

	writeRow(headers)

	seps := make([]string, len(headers))
	for i := range headers {
		seps[i] = strings.Repeat("-", colWidths[i])
	}
	writeRow(seps)

	for _, row := range rows {
		writeRow(row)
	}
}
