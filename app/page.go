package main

import (
	"fmt"
	"io"
	"log"

	"github.com/codecrafters-io/sqlite-starter-go/internal/sqlparser"
	"github.com/codecrafters-io/sqlite-starter-go/internal/utils"
)

type SchemaCell struct {
	PayloadSize uint16
	Type        string
	Name        string
	TableName   string
	RootPage    byte
	SQL         string
}

func parseSchemaCell(reader io.Reader) SchemaCell {
	payloadSize := uint16(utils.ParseUint8(reader))

	if payloadSize > 127 {
		next := utils.ParseUint8(reader)
		payloadSize += uint16(next)

	}

	_ = utils.ParseUint8(reader) // rowId skip

	recordHeaderSize := utils.ParseUint8(reader)
	typeSize := normalizeOrSomething(utils.ParseUint8(reader))
	nameSize := normalizeOrSomething(utils.ParseUint8(reader))
	tableNameSize := normalizeOrSomething(utils.ParseUint8(reader))
	_ = normalizeOrSomething(utils.ParseUint8(reader)) // rootPageSize

	// rest of record header is sql
	sqlSize := 0
	for i := 0; i < int(recordHeaderSize)-5; i++ {
		t := int(utils.ParseUint8(reader))
		sqlSize += t
	}
	sqlSize = normalizeOrSomethingInt(sqlSize)

	actualSqlSize := int(payloadSize) - (1 + int(recordHeaderSize) + int(typeSize) + int(nameSize) + int(tableNameSize) + 1)

	if actualSqlSize > sqlSize {
		sqlSize = actualSqlSize
	}

	// read the actual values
	typeBytes := make([]byte, typeSize)
	reader.Read(typeBytes)
	nameBytes := make([]byte, nameSize)
	reader.Read(nameBytes)
	tableNameBytes := make([]byte, tableNameSize)
	reader.Read(tableNameBytes)
	rootPage := utils.ParseUint8(reader)
	sqlBytes := make([]byte, sqlSize)
	reader.Read(sqlBytes)

	return SchemaCell{
		PayloadSize: payloadSize,
		Type:        string(typeBytes),
		Name:        string(nameBytes),
		TableName:   string(tableNameBytes),
		RootPage:    rootPage,
		SQL:         string(sqlBytes),
	}
}

type Cell struct {
	PayloadSize int
	RowID       int
	Columns     map[string][]byte
}

func parseCell(reader io.Reader, table sqlparser.TableSchema) Cell {
	payloadSizeRaw, _ := utils.ReadVarint(reader)
	payloadSize := int(payloadSizeRaw)
	rowIdRaw, _ := utils.ReadVarint(reader)
	rowId := int(rowIdRaw)

	recordHeaderSize := utils.ParseUint8(reader)

	sizes := []uint64{}
	recordHeadersByteRead := 0
	for recordHeadersByteRead < int(recordHeaderSize)-1 {
		size, bytesRead := utils.ReadVarint(reader)
		sizes = append(sizes, normalizeOrSomethingUint64(size))

		recordHeadersByteRead += bytesRead
	}

	columns := make(map[string][]byte, recordHeaderSize-1)

	if len(table.Columns) != len(sizes) {
		log.Fatal("not one to one in row to columns")
	}

	for i, size := range sizes {
		data := make([]byte, size)
		reader.Read(data)
		columns[table.Columns[i].Name] = data

		if table.Columns[i].IsRowId {
			columns[table.Columns[i].Name] = []byte(fmt.Sprintf("%d", rowId))
		}
	}

	return Cell{
		PayloadSize: payloadSize,
		RowID:       rowId,
		Columns:     columns,
	}
}

type InteriorTablePointerCell struct {
	PageNumber uint32
	RowID      uint16
}

func parseInteriorTableCell(reader io.Reader) InteriorTablePointerCell {
	return InteriorTablePointerCell{
		PageNumber: utils.ParseUint32(reader),
		RowID:      utils.ParseUint16(reader),
	}
}

func normalizeOrSomethingUint64(val uint64) uint64 {
	if val%2 == 0 {
		if val > 12 {
			val -= 12
			val = val / 2
		}
	} else {
		if val > 13 {
			val -= 13
			val = val / 2
		}
	}
	return val
}

func normalizeOrSomethingInt(size int) int {
	if size > 13 {
		size -= 13
		size = size / 2
	}
	return size
}

func normalizeOrSomething(size byte) byte {
	if size > 13 {
		size -= 13
		size = size / 2
	}
	return size
}
