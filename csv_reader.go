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
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
)

var (
	jsonPrinter = func(colCell string) interface{} {
		trimmed := strings.TrimSpace(colCell)
		if len(trimmed) > 1 && ((strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}")) ||
			(strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]"))) {
			var data interface{}
			if err := json.Unmarshal([]byte(trimmed), &data); err != nil {
				return colCell
			}
			return data
		}
		return colCell
	}
	rawPrinter = func(colCell string) interface{} {
		return colCell
	}
)

func processRow(columns, row []string, requiredCols []string, pretty bool) interface{} {
	dataPrinter := rawPrinter
	if pretty {
		dataPrinter = jsonPrinter
	}

	switch len(requiredCols) {
	case 0:
		data := map[string]interface{}{}
		for i, colCell := range row {
			if i < len(columns) {
				data[columns[i]] = dataPrinter(colCell)
			}
		}
		return data
	case 1:
		for i, colCell := range row {
			if i < len(columns) && requiredCols[0] == columns[i] {
				return jsonPrinter(colCell)
			}
		}
		return nil
	default:
		data := map[string]interface{}{}
		for i, colCell := range row {
			if i < len(columns) && lo.Contains(requiredCols, columns[i]) {
				data[columns[i]] = dataPrinter(colCell)
			}
		}
		return data
	}
}

func readCsv(f *os.File, requiredCols []string, limit int, pretty bool) (chan interface{}, error) {
	csvReader := csv.NewReader(f)
	csvReader.LazyQuotes = true

	columns, err := csvReader.Read()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read CSV header")
	}

	if len(columns) == 0 {
		return nil, errors.New("CSV file has no columns")
	}

	if len(columns[0]) >= 3 && columns[0][:3] == CSVHeader {
		columns[0] = columns[0][3:]
	}

	if len(requiredCols) == 1 {
		log.Infof("transfer column %s to json", requiredCols[0])
	} else if len(requiredCols) > 1 {
		log.Infof("transfer columns %v to json", strings.Join(requiredCols, ","))
	} else {
		log.Infof("transfer all columns to json")
	}

	lines := make(chan interface{}, 100)

	go func() {
		var rows int
		defer func() {
			close(lines)
			log.Infof("read %d records", rows)
		}()

		for {
			row, err := csvReader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Errorf("read csv failed: %v", err)
				break
			}

			if len(row) == 0 {
				continue
			}

			rows++
			if limit > 0 && rows > limit {
				break
			}

			result := processRow(columns, row, requiredCols, pretty)
			if result != nil {
				lines <- result
			}
		}
	}()

	return lines, nil
}
