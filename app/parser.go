package main

import (
	"encoding/binary"
	"io"
	"log"
)

func parseUint8(reader io.Reader) uint8 {
	var out uint8
	if err := binary.Read(reader, binary.BigEndian, &out); err != nil {
		log.Fatal("Failed to parse Uint8")
	}
	return out
}

func parseUint16(reader io.Reader) uint16 {
	var out uint16
	if err := binary.Read(reader, binary.BigEndian, &out); err != nil {
		log.Fatal("Failed to parse Uint16")
	}
	return out
}
