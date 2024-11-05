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

	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
)

var (
	jsonPrinter = func(colCell string) interface{} {
		if strings.HasPrefix(colCell, "{") && strings.HasSuffix(colCell, "}") {
			var data interface{}
			if err := json.Unmarshal([]byte(colCell), &data); err != nil {
				log.Fatalf("json unmarshal failed: %v", err)
			}
			return data
		}
		return colCell
	}
	rawPrinter = func(colCell string) interface{} {
		return colCell
	}
)

func getRowReader(lines chan interface{}, requiredCols []string, pretty bool) func(columns, row []string) {
	dataPrinter := rawPrinter
	if pretty {
		dataPrinter = jsonPrinter
	}

	switch len(requiredCols) {
	case 0:
		log.Infof("transfer all columns to json")
		return func(columns, row []string) {
			data := map[string]interface{}{}
			for i, colCell := range row {
				data[columns[i]] = dataPrinter(colCell)
			}
			lines <- data
		}
	case 1:
		log.Infof("transfer column %s to json", requiredCols[0])
		return func(columns, row []string) {
			for i, colCell := range row {
				if requiredCols[0] != columns[i] {
					continue
				}
				lines <- jsonPrinter(colCell)
			}
		}
	default:
		log.Infof("transfer columns %v to json", strings.Join(requiredCols, ","))
		return func(columns, row []string) {
			data := map[string]interface{}{}
			for i, colCell := range row {
				if len(requiredCols) > 0 &&
					!lo.Contains(requiredCols, columns[i]) {
					continue
				}
				data[columns[i]] = dataPrinter(colCell)
				lines <- data
			}
		}
	}
}

func readCsv(f *os.File, requiredCols []string, limit int, pretty bool) (chan interface{}, error) {
	csvReader := csv.NewReader(f)
	csvReader.LazyQuotes = true

	// 读取首行列名
	columns, err := csvReader.Read()
	if err != nil {
		return nil, err
	}

	if len(columns) == 0 {
		return nil, nil
	}

	if columns[0][0:3] == CSVHeader {
		columns[0] = columns[0][4 : len(columns[0])-1] // 去除列名前缀
	}

	lines := make(chan interface{})
	read := getRowReader(lines, requiredCols, pretty)

	go func() {
		var rows int
		defer func() {
			close(lines)
			log.Infof("read %d records", rows)
		}()

		for {
			// 读取CSV文件的下一行数据
			row, err := csvReader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Fatalf("read csv failed: %v", err)
			}

			if len(row) == 0 {
				break
			}

			rows++ // 增加行计数
			if limit > 0 && rows > limit {
				// 如果限制大于0且行数达到限制，跳出循环
				break
			}

			read(columns, row)
		}
	}()

	return lines, nil
}
