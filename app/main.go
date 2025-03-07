package main

import (
	"fmt"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		dbinfo(databaseFile)

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

func dbinfo(dbFile *os.File) {
	headers := parseDBHeaders(dbFile)

	pageHeaders := parsePageHeaders(dbFile)

	// pageHeaders.StartOfCellsContent
	dbFile.Seek(int64(pageHeaders.StartOfCellsContent), 0)

	cell := parseCell(dbFile)
	fmt.Println("First cell:")
	fmt.Println(cell)
	cell = parseCell(dbFile)
	fmt.Println("Second cell:")
	fmt.Println(cell)
	cell = parseCell(dbFile)
	fmt.Println("Third cell:")
	fmt.Println(cell)

	fmt.Printf("database page size: %v\n", headers.PageSize)
	fmt.Printf("number of tables: %v\n", pageHeaders.NumberOfCells)
}
