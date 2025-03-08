package main

import (
	"io"
	"log"
)

type PageType uint8

const (
	InteriorIndexPageType PageType = 2
	InteriorTablePageType          = 5
	LeafIndexPageType              = 10
	LeafTablePageType              = 13
)

type PageHeaders struct {
	Type                uint8
	FreeblockOffset     uint16
	NumberOfCells       uint16
	StartOfCellsContent uint16
	CellAddresses       []uint16
}

func parsePageHeaders(reader io.Reader) PageHeaders {
	pageType := parseUint8(reader)
	freeblockOffset := parseUint16(reader)
	numberOfCells := parseUint16(reader)
	startOfCellsContent := parseUint16(reader)

	reader.Read(make([]byte, 1))

	if pageType != LeafTablePageType {
		log.Fatal("Page type is not LeafTablePageType")
	}

	cellAddresses := make([]uint16, numberOfCells)

	for i := 0; i < int(numberOfCells); i++ {
		cellAddresses[i] = parseUint16(reader)
	}

	return PageHeaders{
		Type:                pageType,
		FreeblockOffset:     freeblockOffset,
		NumberOfCells:       numberOfCells,
		StartOfCellsContent: startOfCellsContent,
		CellAddresses:       cellAddresses,
	}
}
