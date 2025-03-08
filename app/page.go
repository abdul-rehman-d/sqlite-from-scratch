package main

import (
	"io"
)

type SchemaCell struct {
	PayloadSize byte
	Type        string
	Name        string
	TableName   string
	RootPage    byte
	SQL         string
}

func parseCell(reader io.Reader) SchemaCell {
	payloadSize := parseUint8(reader)
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
