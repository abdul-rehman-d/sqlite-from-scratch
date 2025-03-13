package dbfile

import (
	"io"

	"github.com/codecrafters-io/sqlite-starter-go/internal/utils"
)

type DBHeaders struct {
	PageSize uint16
}

func ParseDBHeaders(file io.Reader) DBHeaders {
	version := make([]byte, 16)
	file.Read(version)

	pageSize := utils.ParseUint16(file)

	rest := make([]byte, 100-18)
	file.Read(rest)

	return DBHeaders{
		PageSize: pageSize,
	}
}
