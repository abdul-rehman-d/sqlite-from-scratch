package utils

import (
	"encoding/binary"
	"io"
	"log"
)

func ParseUint8(reader io.Reader) uint8 {
	var out uint8
	if err := binary.Read(reader, binary.BigEndian, &out); err != nil {
		log.Fatal("Failed to parse Uint8")
	}
	return out
}

func ParseUint16(reader io.Reader) uint16 {
	var out uint16
	if err := binary.Read(reader, binary.BigEndian, &out); err != nil {
		log.Fatal("Failed to parse Uint16")
	}
	return out
}

func ParseUint32(reader io.Reader) uint32 {
	var out uint32
	if err := binary.Read(reader, binary.BigEndian, &out); err != nil {
		log.Fatal("Failed to parse Uint32")
	}
	return out
}

func ReadVarint(r io.Reader) (uint64, int) {
	var result uint64
	var bytesRead int = 0

	for i := 0; i < 9; i++ { // SQLite varints are at most 9 bytes
		// Read a single byte
		buf := make([]byte, 1)
		r.Read(buf)

		bytesRead++

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

	return result, bytesRead
}
