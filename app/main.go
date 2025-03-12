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

type Database struct {
	File        *os.File
	Headers     DBHeaders
	Params      *SelectStatementResult
	TableSchema *TableSchema
	CountOnly   bool
	Count       uint64
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

	countOnly := false

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

	if len(params.Columns) > 0 && len(params.Columns[0]) >= 5 && strings.ToLower(params.Columns[0][0:5]) == "count" {
		countOnly = true
	} else if err := validateAllColumnNames(params, tableSchema); err != nil {
		log.Fatal(err)
	}

	db := Database{
		File:        databaseFile,
		Headers:     headers,
		Params:      params,
		TableSchema: tableSchema,
		CountOnly:   countOnly,
		Count:       uint64(0),
	}

	db.ParsePage(int64(tableCell.RootPage))

	if countOnly {
		fmt.Println(db.Count)
	}
}

func (db *Database) ParsePage(pageNumber int64) {
	offset := (int64(pageNumber) - 1) * int64(db.Headers.PageSize)

	db.File.Seek(offset, 0)
	pageHeaders := parsePageHeaders(db.File)

	switch pageHeaders.Type {
	case InteriorTablePageType:
		for _, pageCellAddress := range pageHeaders.CellAddresses {
			db.File.Seek(int64(offset), 0)
			db.File.Seek(int64(pageCellAddress), 1)
			pageCell := parseInteriorTableCell(db.File)

			db.ParsePage(int64(pageCell.PageNumber))
		}
		db.ParsePage(int64(pageHeaders.RightMostPageNumber))
	case LeafTablePageType:
		for _, cellAddress := range pageHeaders.CellAddresses {
			db.File.Seek(int64(offset), 0)
			db.File.Seek(int64(cellAddress), 1)
			cell := parseCell(db.File, *db.TableSchema)

			db.FilterAndPrintCell(cell)
		}
	}
}

func validateAllColumnNames(params *SelectStatementResult, tableSchema *TableSchema) error {
	if params.AllColumns {
		return nil
	}
	for _, column := range params.Columns {
		found := false
		for _, col := range tableSchema.Columns {
			if col.Name == strings.ToLower(column) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("column not found: %s\n", column)
		}
	}
	return nil
}

func (db *Database) FilterAndPrintCell(cell Cell) {
	skip := false

	for _, where := range db.Params.Where {
		val := cell.Columns[where.ColumnName]
		if !compareByteArrays(val, where.ValueToCompare) {
			skip = true
			break

		}
	}

	if skip {
		return
	}

	db.Count++

	if db.CountOnly {
		return
	}

	if db.Params.AllColumns {
		for i, col := range db.TableSchema.Columns {
			fmt.Print(string(cell.Columns[col.Name]))
			if i != len(db.TableSchema.Columns)-1 {
				fmt.Print("|")
			}
		}
	} else {
		for i, col := range db.Params.Columns {
			fmt.Print(string(cell.Columns[col]))
			if i != len(db.Params.Columns)-1 {
				fmt.Print("|")
			}
		}
	}
	fmt.Print("\n")
}
