package utils

import (
	"akita/db"
	"hash/crc32"
)

func GetCrc32(header *db.DataHeader, key []byte, value []byte) uint32 {
	//使用 header + key + value 的 byte 数组 形成 crc32
	ksBuf, _ := IntToByteSlice(header.Ks)
	vsBuf, _ := IntToByteSlice(header.Vs)
	flagBuf, _ := IntToByteSlice(header.Flag)
	buf := append(ksBuf, vsBuf...)
	buf = append(buf, flagBuf...)
	buf = append(buf, key...)
	buf = append(buf, value...)
	crcValue := crc32.ChecksumIEEE(buf)
	return crcValue
}