package db

import (
	"os"
)

type DB struct {
	File    *os.File
	Headers DBHeaders
	Tables  []Table
	Indexes []Index
}

func NewDB(file *os.File) *DB {

	db := &DB{
		File:    file,
		Headers: ParseDBHeaders(file),
		Tables:  []Table{},
		Indexes: []Index{},
	}

	pageHeaders := ParsePageHeaders(file)

	for _, cellAddress := range pageHeaders.CellAddresses {
		file.Seek(int64(cellAddress), 0)
		cell := ParseSchemaCell(file)

		if cell.Type == "table" {
			table := Table{
				Name: cell.TableName,
			}
			db.Tables = append(db.Tables, table)
		} else if cell.Type == "index" {
			index := Index{
				Name: cell.Name,
			}
			db.Indexes = append(db.Indexes, index)
		}
	}

	return db
}
