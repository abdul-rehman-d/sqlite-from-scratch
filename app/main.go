package main

import (
	"fmt"
	"log"
	"os"
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

	switch command {
	case ".dbinfo":
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		dbinfo(databaseFile)

	case ".tables":
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

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
