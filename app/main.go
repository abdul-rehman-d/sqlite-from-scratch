package main

import (
	"bytes"
	"encoding/binary"
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
	header := make([]byte, 100)

	_, err := dbFile.Read(header)
	if err != nil {
		log.Fatal(err)
	}

	var pageSize uint16
	if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize); err != nil {
		fmt.Println("Failed to read integer:", err)
		return
	}

	page1 := make([]byte, pageSize)

	_, err = dbFile.Read(page1)
	if err != nil {
		log.Fatal(err)
	}

	numberOfTables := 0

	var numberOfCells uint16
	if err := binary.Read(bytes.NewReader(page1[3:5]), binary.BigEndian, &numberOfCells); err != nil {
		fmt.Println("Failed to read integer:", err)
		return
	}

	for i := 0; i < int(numberOfCells); i++ {
		// cell pointer

		startIdx := 8 + (i * 2)
		endIdx := startIdx + 2

		var cellPointer uint16
		if err := binary.Read(bytes.NewReader(page1[startIdx:endIdx]), binary.BigEndian, &cellPointer); err != nil {
			fmt.Println("Failed to read integer:", err)
			return
		}

		cellPointer -= 100

		payloadSize := page1[cellPointer]
		headerSize := page1[cellPointer+2]

		start := cellPointer + 2 + uint16(headerSize)

		cell := page1[start : start+uint16(payloadSize)]

		typee := string(cell[0:5])

		switch typee {
		case "table":
			numberOfTables++
		case "index":
		default:
			fmt.Println("there could only be table or index")
			return
		}
	}

	fmt.Printf("database page size: %v\n", pageSize)
	fmt.Printf("number of tables: %v\n", numberOfTables)
}
