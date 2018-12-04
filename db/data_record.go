package db

import (
	"akita/utils"
	"fmt"
)

type (
	DataHeader struct {
		Ks   int32  // key size
		Vs   int32  // value size
		Crc  uint32 // judge the file is
		Flag int32  // type of write to file
	}

	// 表示写入数据库文件的数据结构
	DataRecord struct {
		dateHeader *DataHeader
		key   []byte
		value []byte
	}
)

func (record *DataRecord) WriteRecord (dataFile string, offset int) (bool, error) {	// 将记录写入
	ksBuf, err := utils.IntToByteSlice(record.dateHeader.Ks)
	if err != nil {
		return false, fmt.Errorf(err)
	}
	vsBuf, err := utils.IntToByteSlice(record.dateHeader.Vs)
	flagBuf, err := utils.IntToByteSlice(record.dateHeader.Flag)
	recordBuf := utils.AppendByteSlice(ksBuf, vsBuf, flagBuf, record.key, record.value)
	crc32  := utils.CreateCrc32(recordBuf)
	crcBuf, _ := utils.UintToByteSlice(crc32)
	recordBuf = append(recordBuf, crcBuf...)
	return false, nil
}

