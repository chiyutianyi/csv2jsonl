# csv2jsonl

This is a simple utility that converts a CSV file to a JSONL (JSON Lines) file.

You can ever convert all columns or only specific ones. If only one column is selected, the output will be a JSON object with the value of the selected column.

# Install
```bash
go get github.com/chiyutianyi/csv2jsonl
```

# Usage
```bash
csv2jsonl -i <input_file> -o <output_file>
```