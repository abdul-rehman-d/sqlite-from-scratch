package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
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
		cell := parseCell(dbFile)
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
		cell := parseCell(dbFile)
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
	// count command
	splitCommand := strings.Split(command, " ")
	lastWord := splitCommand[len(splitCommand)-1]
	if lastWord[len(lastWord)-1] == ';' {
		lastWord = lastWord[:len(lastWord)-1]
	}

	tableName := lastWord
	headers := parseDBHeaders(databaseFile)

	pageHeaders := parsePageHeaders(databaseFile)

	found := false
	tableCell := SchemaCell{}

	for _, cellAddress := range pageHeaders.CellAddresses {
		databaseFile.Seek(int64(cellAddress), 0)
		cell := parseCell(databaseFile)
		if cell.Type == "table" && cell.TableName == tableName {
			tableCell = cell
			found = true
		}
	}
	if !found {
		fmt.Println("Table not found")
		os.Exit(1)
	}

	offset := (uint16(tableCell.RootPage) - 1) * headers.PageSize

	databaseFile.Seek(int64(offset), 0)

	tablePageHeaders := parsePageHeaders(databaseFile)
	fmt.Println(tablePageHeaders.NumberOfCells)
}
