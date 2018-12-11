package common

import (
	"hash/crc32"
)

func CreateCrc32(buf []byte) uint32 {
	crcValue := crc32.ChecksumIEEE(buf)
	return crcValue
}

