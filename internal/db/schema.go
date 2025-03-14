package db

import (
	"fmt"
	"io"

	"github.com/codecrafters-io/sqlite-starter-go/internal/utils"
)

type Table struct {
	Name string
}

type Index struct {
	Name string
}

type SchemaCell struct {
	PayloadSize uint64
	Type        string
	Name        string
	TableName   string
	RootPage    byte
	SQL         string
}

type UsignedInteger interface {
	uint8 | uint16 | uint32 | uint64
}

func ParseSchemaCell(reader io.Reader) SchemaCell {
	payloadSize, _ := utils.ReadVarint(reader)

	utils.ReadVarint(reader) // rowId skip

	recordHeaderSize := utils.ParseUint8(reader)

	typeSize := normalizeOrSomething(utils.ParseUint8(reader))
	nameSize := normalizeOrSomething(utils.ParseUint8(reader))
	tableNameSize := normalizeOrSomething(utils.ParseUint8(reader))
	_ = normalizeOrSomething(utils.ParseUint8(reader)) // rootPageSize

	sqlSize, n := utils.ReadVarint(reader)

	if n != int(recordHeaderSize)-5 {
		panic(fmt.Sprintf("some overflow %d", n))
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

func normalizeOrSomething[T UsignedInteger](size T) T {
	if size > 12 && size%2 == 0 {
		size -= 12
		size = size / 2
	}
	if size > 13 && size%2 == 1 {
		size -= 13
		size = size / 2
	}
	return size
}
