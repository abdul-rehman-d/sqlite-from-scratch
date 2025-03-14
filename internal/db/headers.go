package db

import (
	"io"

	"github.com/codecrafters-io/sqlite-starter-go/internal/utils"
)

const (
	// DB HEADER RELATED CONSTANTS
	VersionBytes      = 16
	PageSizeBytes     = 2
	TotalHeadderBytes = 100

	// PAGE HEADER RELATED CONSTANTS
	InteriorIndexPageType uint8 = 2
	InteriorTablePageType uint8 = 5
	LeafIndexPageType     uint8 = 10
	LeafTablePageType     uint8 = 13
)

type DBHeaders struct {
	PageSize uint16
}

func ParseDBHeaders(file io.Reader) DBHeaders {
	version := make([]byte, 16)
	file.Read(version)

	pageSize := utils.ParseUint16(file)

	rest := make([]byte, TotalHeadderBytes-VersionBytes-PageSizeBytes)
	file.Read(rest)

	return DBHeaders{
		PageSize: pageSize,
	}
}

const ()

type PageHeaders struct {
	Type                uint8
	FreeblockOffset     uint16
	NumberOfCells       uint16
	StartOfCellsContent uint16
	CellAddresses       []uint16
	RightMostPageNumber uint32
}

func ParsePageHeaders(reader io.Reader) PageHeaders {
	rightMostPageNumber := uint32(0)

	pageType := utils.ParseUint8(reader)
	freeblockOffset := utils.ParseUint16(reader)
	numberOfCells := utils.ParseUint16(reader)
	startOfCellsContent := utils.ParseUint16(reader)

	reader.Read(make([]byte, 1))

	if pageType == InteriorTablePageType || pageType == InteriorIndexPageType {
		rightMostPageNumber = utils.ParseUint32(reader)
	}

	cellAddresses := make([]uint16, numberOfCells)

	for i := 0; i < int(numberOfCells); i++ {
		cellAddresses[i] = utils.ParseUint16(reader)
	}

	return PageHeaders{
		Type:                pageType,
		FreeblockOffset:     freeblockOffset,
		NumberOfCells:       numberOfCells,
		StartOfCellsContent: startOfCellsContent,
		CellAddresses:       cellAddresses,
		RightMostPageNumber: rightMostPageNumber,
	}
}
