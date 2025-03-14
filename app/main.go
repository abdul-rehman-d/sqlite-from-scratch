package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/codecrafters-io/sqlite-starter-go/internal/db"
	"github.com/codecrafters-io/sqlite-starter-go/internal/dbfile"
	"github.com/codecrafters-io/sqlite-starter-go/internal/sqlparser"
	"github.com/codecrafters-io/sqlite-starter-go/internal/utils"
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

	db := db.NewDB(databaseFile)

	switch command {
	case ".dbinfo":
		dbinfo(db)

	case ".tables":
		tables(db)

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

func dbinfo(db *db.DB) {
	fmt.Printf("database page size: %v\n", db.Headers.PageSize)
	fmt.Printf("number of tables: %v\n", len(db.Tables))
}

func tables(db *db.DB) {
	for i, table := range db.Tables {
		fmt.Print(table.Name)
		if i != len(db.Tables)-1 {
			fmt.Print(" ")
		}
	}
}

type Database struct {
	File        *os.File
	Headers     dbfile.DBHeaders
	Params      *sqlparser.SelectStatementResult
	TableSchema *sqlparser.TableSchema
	CountOnly   bool
	Count       uint64
}

func executeSQL(databaseFile *os.File, command string) {
	params, err := sqlparser.ParseSelectStatement(command)
	if err != nil {
		log.Fatal(err)
	}

	headers := dbfile.ParseDBHeaders(databaseFile)

	pageHeaders := dbfile.ParsePageHeaders(databaseFile)

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

	tableSchema, err := sqlparser.ParseTableSchema(tableCell.SQL)
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
	pageHeaders := dbfile.ParsePageHeaders(db.File)

	switch pageHeaders.Type {
	case dbfile.InteriorTablePageType:
		for _, pageCellAddress := range pageHeaders.CellAddresses {
			db.File.Seek(int64(offset), 0)
			db.File.Seek(int64(pageCellAddress), 1)
			pageCell := parseInteriorTableCell(db.File)

			db.ParsePage(int64(pageCell.PageNumber))
		}
		db.ParsePage(int64(pageHeaders.RightMostPageNumber))
	case dbfile.LeafTablePageType:
		for _, cellAddress := range pageHeaders.CellAddresses {
			db.File.Seek(int64(offset), 0)
			db.File.Seek(int64(cellAddress), 1)
			cell := parseCell(db.File, *db.TableSchema)

			db.FilterAndPrintCell(cell)
		}
	}
}

func validateAllColumnNames(params *sqlparser.SelectStatementResult, tableSchema *sqlparser.TableSchema) error {
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

	if where := db.Params.Where; where != nil {
		val := cell.Columns[where.ColumnName]
		if !utils.CompareByteArrays(val, where.ValueToCompare) {
			skip = true
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
