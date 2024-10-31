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
	"flag"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

var CSVHeader = string([]byte{0xef, 0xbb, 0xbf})

func main() {
	var enc *json.Encoder
	i := flag.String("i", "", "input csv file")
	o := flag.String("o", "", "output jsonl file")

	loggerLevel := flag.String("logger_level", "info", "log level")
	limit := flag.Int("limit", 0, "limit")
	pretty := flag.Bool("pretty", false, "output format pretty")
	columns := flag.String("columns", "", "columns to print, default as all")

	help := flag.Bool("help", false, "print help")

	flag.Parse()

	if *help || *i == "" {
		flag.Usage()
		return
	}

	level, err := log.ParseLevel(*loggerLevel)
	if err != nil {
		level = log.InfoLevel
	}
	log.SetLevel(level)

	var cols []string
	if *columns != "" {
		cols = strings.Split(*columns, ",")
	}

	f, err := os.OpenFile(*i, os.O_RDONLY, 0o644) // 打开文件，只读模式，权限为0o644
	if err != nil {
		log.Fatalf("open file failed: %v", err)
	}

	defer func() {
		if err := f.Close(); err != nil {
			log.Fatalf("close file failed: %v", err)
		}
	}()

	lines, err := readCsv(f, cols, *limit)
	if err != nil {
		log.Fatalf("read csv failed: %v", err)
	}

	if *o == "" {
		enc = json.NewEncoder(os.Stdout)
	} else {
		f, err := os.OpenFile(*o, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			log.Fatalf("open file failed: %v", err)
		}
		defer f.Close()
		enc = json.NewEncoder(f)
	}

	enc.SetEscapeHTML(false)
	if *pretty {
		enc.SetIndent("", "  ")
	}

	for line := range lines {
		enc.Encode(line)
	}
}
