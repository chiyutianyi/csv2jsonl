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
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcessRow(t *testing.T) {
	testCases := []struct {
		Name         string
		Columns      []string
		Row          []string
		RequiredCols []string
		Pretty       bool
		Expected     interface{}
	}{
		{
			Name:         "all columns",
			Columns:      []string{"name", "age", "city"},
			Row:          []string{"Alice", "25", "New York"},
			RequiredCols: []string{},
			Pretty:       false,
			Expected:     map[string]interface{}{"name": "Alice", "age": "25", "city": "New York"},
		},
		{
			Name:         "single column",
			Columns:      []string{"name", "age", "city"},
			Row:          []string{"Alice", "25", "New York"},
			RequiredCols: []string{"name"},
			Pretty:       false,
			Expected:     "Alice",
		},
		{
			Name:         "multiple columns",
			Columns:      []string{"name", "age", "city"},
			Row:          []string{"Alice", "25", "New York"},
			RequiredCols: []string{"name", "city"},
			Pretty:       false,
			Expected:     map[string]interface{}{"name": "Alice", "city": "New York"},
		},
		{
			Name:         "single column with JSON",
			Columns:      []string{"data"},
			Row:          []string{`{"key": "value"}`},
			RequiredCols: []string{"data"},
			Pretty:       true,
			Expected:     map[string]interface{}{"key": "value"},
		},
		{
			Name:         "non-existent column",
			Columns:      []string{"name", "age"},
			Row:          []string{"Alice", "25"},
			RequiredCols: []string{"nonexistent"},
			Pretty:       false,
			Expected:     nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			result := processRow(testCase.Columns, testCase.Row, testCase.RequiredCols, testCase.Pretty)
			require.Equal(t, testCase.Expected, result)
		})
	}
}

func TestReadCsv(t *testing.T) {
	testCases := []struct {
		Name         string
		CSVContent   string
		RequiredCols []string
		Limit        int
		Pretty       bool
		ExpectedRows int
		Validate     func(t *testing.T, results []interface{})
	}{
		{
			Name:         "basic CSV",
			CSVContent:   "name,age,city\nAlice,25,New York\nBob,30,London",
			RequiredCols: []string{},
			Limit:        0,
			Pretty:       false,
			ExpectedRows: 2,
			Validate: func(t *testing.T, results []interface{}) {
				require.Len(t, results, 2)
				row1 := results[0].(map[string]interface{})
				require.Equal(t, "Alice", row1["name"])
				require.Equal(t, "25", row1["age"])
				require.Equal(t, "New York", row1["city"])
			},
		},
		{
			Name:         "CSV with BOM",
			CSVContent:   string([]byte{0xef, 0xbb, 0xbf}) + "name,age\nAlice,25",
			RequiredCols: []string{},
			Limit:        0,
			Pretty:       false,
			ExpectedRows: 1,
			Validate: func(t *testing.T, results []interface{}) {
				require.Len(t, results, 1)
				row1 := results[0].(map[string]interface{})
				require.Equal(t, "Alice", row1["name"])
			},
		},
		{
			Name:         "single column selection",
			CSVContent:   "name,age,city\nAlice,25,New York\nBob,30,London",
			RequiredCols: []string{"name"},
			Limit:        0,
			Pretty:       false,
			ExpectedRows: 2,
			Validate: func(t *testing.T, results []interface{}) {
				require.Len(t, results, 2)
				require.Equal(t, "Alice", results[0])
				require.Equal(t, "Bob", results[1])
			},
		},
		{
			Name:         "with limit",
			CSVContent:   "name,age\nAlice,25\nBob,30\nCharlie,35",
			RequiredCols: []string{},
			Limit:        2,
			Pretty:       false,
			ExpectedRows: 2,
			Validate: func(t *testing.T, results []interface{}) {
				require.Len(t, results, 2)
			},
		},
		{
			Name:         "CSV with JSON data",
			CSVContent:   "name,data\nAlice,\"{\"\"key\"\":\"\"value\"\"}\"\nBob,plain_text",
			RequiredCols: []string{},
			Limit:        0,
			Pretty:       true,
			ExpectedRows: 2,
			Validate: func(t *testing.T, results []interface{}) {
				require.Len(t, results, 2)
				row1 := results[0].(map[string]interface{})
				require.Equal(t, "Alice", row1["name"])
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tmpFile, err := os.CreateTemp("", "test_*.csv")
			require.NoError(t, err)
			defer os.Remove(tmpFile.Name())

			_, err = tmpFile.WriteString(testCase.CSVContent)
			require.NoError(t, err)
			tmpFile.Close()

			file, err := os.Open(tmpFile.Name())
			require.NoError(t, err)
			defer file.Close()

			lines, err := readCsv(file, testCase.RequiredCols, testCase.Limit, testCase.Pretty)
			require.NoError(t, err)

			var results []interface{}
			for line := range lines {
				results = append(results, line)
			}

			require.Len(t, results, testCase.ExpectedRows)
			if testCase.Validate != nil {
				testCase.Validate(t, results)
			}
		})
	}
}

func TestReadCsvErrors(t *testing.T) {
	testCases := []struct {
		Name       string
		CSVContent string
		ShouldFail bool
	}{
		{
			Name:       "empty file",
			CSVContent: "",
			ShouldFail: true,
		},
		{
			Name:       "only header",
			CSVContent: "name,age",
			ShouldFail: false,
		},
		{
			Name:       "malformed CSV",
			CSVContent: "name,age\nAlice,25,extra_field",
			ShouldFail: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tmpFile, err := os.CreateTemp("", "test_*.csv")
			require.NoError(t, err)
			defer os.Remove(tmpFile.Name())

			_, err = tmpFile.WriteString(testCase.CSVContent)
			require.NoError(t, err)
			tmpFile.Close()

			file, err := os.Open(tmpFile.Name())
			require.NoError(t, err)
			defer file.Close()

			lines, err := readCsv(file, []string{}, 0, false)
			if testCase.ShouldFail {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if lines != nil {
					for range lines {
					}
				}
			}
		})
	}
}

func TestJsonPrinter(t *testing.T) {
	testCases := []struct {
		Name     string
		Input    string
		Expected interface{}
	}{
		{
			Name:     "valid JSON object",
			Input:    `{"key": "value"}`,
			Expected: map[string]interface{}{"key": "value"},
		},
		{
			Name:     "valid JSON array",
			Input:    `[1, 2, 3]`,
			Expected: []interface{}{float64(1), float64(2), float64(3)},
		},
		{
			Name:     "plain text",
			Input:    "plain text",
			Expected: "plain text",
		},
		{
			Name:     "invalid JSON",
			Input:    `{"invalid": json}`,
			Expected: `{"invalid": json}`,
		},
		{
			Name:     "JSON with whitespace",
			Input:    `  {"key": "value"}  `,
			Expected: map[string]interface{}{"key": "value"},
		},
		{
			Name:     "empty string",
			Input:    "",
			Expected: "",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			result := jsonPrinter(testCase.Input)
			require.Equal(t, testCase.Expected, result)
		})
	}
}
