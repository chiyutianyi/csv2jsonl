/*
 * Copyright 2024 Han Xin, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMainIntegration(t *testing.T) {
	testCases := []struct {
		Name         string
		InputFile    string
		Columns      []string
		Limit        int
		Pretty       bool
		ExpectedRows int
		Validate     func(t *testing.T, output string)
	}{
		{
			Name:         "basic CSV conversion",
			InputFile:    "testdata/simple.csv",
			Columns:      []string{},
			Limit:        0,
			Pretty:       false,
			ExpectedRows: 2,
			Validate: func(t *testing.T, output string) {
				lines := strings.Split(strings.TrimSpace(output), "\n")
				require.Len(t, lines, 2)

				var row1 map[string]interface{}
				err := json.Unmarshal([]byte(lines[0]), &row1)
				require.NoError(t, err)
				require.Equal(t, "Alice", row1["name"])
				require.Equal(t, "25", row1["age"])
			},
		},
		{
			Name:         "single column selection",
			InputFile:    "testdata/simple.csv",
			Columns:      []string{"name"},
			Limit:        0,
			Pretty:       false,
			ExpectedRows: 2,
			Validate: func(t *testing.T, output string) {
				lines := strings.Split(strings.TrimSpace(output), "\n")
				require.Len(t, lines, 2)
				require.Equal(t, `"Alice"`, lines[0])
				require.Equal(t, `"Bob"`, lines[1])
			},
		},
		{
			Name:         "multiple column selection",
			InputFile:    "testdata/simple.csv",
			Columns:      []string{"name", "city"},
			Limit:        0,
			Pretty:       false,
			ExpectedRows: 2,
			Validate: func(t *testing.T, output string) {
				lines := strings.Split(strings.TrimSpace(output), "\n")
				require.Len(t, lines, 2)

				var row1 map[string]interface{}
				err := json.Unmarshal([]byte(lines[0]), &row1)
				require.NoError(t, err)
				require.Equal(t, "Alice", row1["name"])
				require.Equal(t, "New York", row1["city"])
				require.NotContains(t, row1, "age")
			},
		},
		{
			Name:         "limit rows",
			InputFile:    "testdata/simple.csv",
			Columns:      []string{},
			Limit:        1,
			Pretty:       false,
			ExpectedRows: 1,
			Validate: func(t *testing.T, output string) {
				lines := strings.Split(strings.TrimSpace(output), "\n")
				require.Len(t, lines, 1)
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tmpDir := t.TempDir()
			outputFile := filepath.Join(tmpDir, "output.jsonl")

			inputFile, err := os.Open(testCase.InputFile)
			require.NoError(t, err)
			defer inputFile.Close()

			lines, err := readCsv(inputFile, testCase.Columns, testCase.Limit, testCase.Pretty)
			require.NoError(t, err)

			outFile, err := os.Create(outputFile)
			require.NoError(t, err)
			defer outFile.Close()

			enc := json.NewEncoder(outFile)
			enc.SetEscapeHTML(false)
			if testCase.Pretty {
				enc.SetIndent("", "  ")
			}

			var count int
			for line := range lines {
				err := enc.Encode(line)
				require.NoError(t, err)
				count++
			}

			require.Equal(t, testCase.ExpectedRows, count)

			output, err := os.ReadFile(outputFile)
			require.NoError(t, err)

			if testCase.Validate != nil {
				testCase.Validate(t, string(output))
			}
		})
	}
}
