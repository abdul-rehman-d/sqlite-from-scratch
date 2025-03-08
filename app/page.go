package main

import (
	"io"
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
	PayloadSize uint16
	RowID       byte
	Columns     []string
}

func parseCell(reader io.Reader) Cell {
	payloadSize := uint16(parseUint8(reader))
	if payloadSize > 127 {
		next := parseUint8(reader)
		payloadSize += uint16(next)

	}
	rowId := parseUint8(reader)

	recordHeaderSize := parseUint8(reader)

	sizes := make([]byte, recordHeaderSize-1)
	for i := 0; i < int(recordHeaderSize)-1; i++ {
		sizes[i] = normalizeOrSomething(parseUint8(reader))
	}

	columns := make([]string, recordHeaderSize-1)

	for i, size := range sizes {
		data := make([]byte, size)
		reader.Read(data)
		columns[i] = string(data)
	}

	return Cell{
		PayloadSize: payloadSize,
		RowID:       rowId,
		Columns:     columns,
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
