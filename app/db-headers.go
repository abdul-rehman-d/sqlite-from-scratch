package main

import "io"

type DBHeaders struct {
	PageSize uint16
}

func parseDBHeaders(reader io.Reader) DBHeaders {
	version := make([]byte, 16)
	reader.Read(version)

	pageSize := parseUint16(reader)

	rest := make([]byte, 100-18)
	reader.Read(rest)

	return DBHeaders{
		PageSize: pageSize,
	}
}
