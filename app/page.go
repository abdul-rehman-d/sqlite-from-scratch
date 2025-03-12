package main

import (
	"fmt"
	"io"
	"log"
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
	payloadSize := uint16(parseUint8(reader))

	if payloadSize > 127 {
		next := parseUint8(reader)
		payloadSize += uint16(next)

	}

	_ = parseUint8(reader) // rowId skip

	recordHeaderSize := parseUint8(reader)
	typeSize := normalizeOrSomething(parseUint8(reader))
	nameSize := normalizeOrSomething(parseUint8(reader))
	tableNameSize := normalizeOrSomething(parseUint8(reader))
	_ = normalizeOrSomething(parseUint8(reader)) // rootPageSize

	// rest of record header is sql
	sqlSize := 0
	for i := 0; i < int(recordHeaderSize)-5; i++ {
		t := int(parseUint8(reader))
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
	rootPage := parseUint8(reader)
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

func readCellHeaderValue(reader io.Reader) int {
	out := 0
	for {
		next := parseUint8(reader)
		out += int(next)
		if next <= 127 {
			break
		}
	}
	return out
}

func ReadVarint(r io.Reader) uint64 {
	var result uint64

	for i := 0; i < 9; i++ { // SQLite varints are at most 9 bytes
		// Read a single byte
		buf := make([]byte, 1)
		r.Read(buf)

		b := buf[0]

		// Extract lower 7 bits and shift into place
		if i == 8 { // Last byte (9th byte) stores full 8 bits
			result = (result << 8) | uint64(b)
			break
		} else {
			result = (result << 7) | uint64(b&0x7F)
		}

		// If MSB is 0, stop reading (this was the last byte)
		if b&0x80 == 0 {
			break
		}
	}

	return result
}

func parseCell(reader io.Reader, table TableSchema) Cell {
	payloadSize := readCellHeaderValue(reader)
	rowId := int(ReadVarint(reader))

	recordHeaderSize := parseUint8(reader)

	sizes := make([]byte, recordHeaderSize-1)
	for i := 0; i < int(recordHeaderSize)-1; i++ {
		sizes[i] = normalizeOrSomething(parseUint8(reader))
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
		PageNumber: parseUint32(reader),
		RowID:      parseUint16(reader),
	}
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
