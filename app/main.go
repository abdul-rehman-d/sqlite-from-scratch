package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: dbinfo <database file> <command>")
		os.Exit(1)
	}

	databaseFilePath := os.Args[1]
	command := os.Args[2]

	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}

	if command[0] != '.' {
		executeSQL(databaseFile, command)
		return
	}

	switch command {
	case ".dbinfo":
		dbinfo(databaseFile)

	case ".tables":
		tables(databaseFile)

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

func dbinfo(dbFile *os.File) {
	headers := parseDBHeaders(dbFile)

	pageHeaders := parsePageHeaders(dbFile)

	numberOfTables := 0

	for _, cellAddress := range pageHeaders.CellAddresses {
		dbFile.Seek(int64(cellAddress), 0)
		cell := parseSchemaCell(dbFile)

		if cell.Type == "table" {
			numberOfTables++
		}
	}

	fmt.Printf("database page size: %v\n", headers.PageSize)
	fmt.Printf("number of tables: %v\n", numberOfTables)
}

func tables(dbFile *os.File) {
	_ = parseDBHeaders(dbFile)

	pageHeaders := parsePageHeaders(dbFile)

	flag := false

	for _, cellAddress := range pageHeaders.CellAddresses {
		dbFile.Seek(int64(cellAddress), 0)
		cell := parseSchemaCell(dbFile)
		if cell.Type == "table" {
			if flag {
				fmt.Printf(" ")
			}
			fmt.Printf("%v", cell.TableName)
			flag = true
		}
	}
}

func executeSQL(databaseFile *os.File, command string) {
	stmt, err := sqlparser.Parse(command)
	if err != nil {
		log.Fatal(err)
	}

	if _, ok := stmt.(*sqlparser.Select); !ok {
		log.Fatal("no commands but `select` is supported yet")
	}

	params, err := parseSelectStatement(stmt)
	if err != nil {
		log.Fatal(err)
	}

	headers := parseDBHeaders(databaseFile)

	pageHeaders := parsePageHeaders(databaseFile)

	found := false
	tableCell := SchemaCell{}

	for _, cellAddress := range pageHeaders.CellAddresses {
		databaseFile.Seek(int64(cellAddress), 0)
		test := make([]byte, 20)
		databaseFile.Read(test)

		databaseFile.Seek(int64(cellAddress), 0)
		cell := parseSchemaCell(databaseFile)
		if cell.Type == "table" && cell.TableName == params.TableName {
			tableCell = cell
			found = true
		}
	}
	if !found {
		log.Fatal("Table not found")
	}

	tableSchema, err := parseTableSchema(tableCell.SQL)
	if err != nil {
		log.Fatal(err)
	}

	offset := (uint16(tableCell.RootPage) - 1) * headers.PageSize

	databaseFile.Seek(int64(offset), 0)

	tablePageHeaders := parsePageHeaders(databaseFile)

	if len(params.Columns) > 0 && len(params.Columns[0]) >= 5 && strings.ToLower(params.Columns[0][0:5]) == "count" {
		fmt.Println(tablePageHeaders.NumberOfCells)
		return
	}

	indices := make([]int, len(params.Columns))
	for i, column := range params.Columns {
		found := false
		for j, col := range tableSchema.Columns {
			if col.Name == column {
				found = true
				indices[i] = j
				break
			}
		}
		if !found {
			log.Fatalf("column not found: %s\n", column)
		}
	}

	for _, cellAddress := range tablePageHeaders.CellAddresses {
		databaseFile.Seek(int64(offset), 0)
		databaseFile.Seek(int64(cellAddress), 1)
		cell := parseCell(databaseFile)

		for i, idx := range indices {
			fmt.Print(cell.Columns[idx])
			if i != len(indices)-1 {
				fmt.Print("|")
			}
		}
		fmt.Print("\n")
	}
}
