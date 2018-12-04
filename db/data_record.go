package db

import (
	"akita/common"
	"akita/utils"
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

// 向数据文件中写入一条记录
func (record *DataRecord) WriteRecord (dataFile string, offset int64) (int64, error) {	// 将记录写入
	ksBuf, err := utils.IntToByteSlice(record.dateHeader.Ks)
	if err != nil {
		return 0, err
	}
	vsBuf, err := utils.IntToByteSlice(record.dateHeader.Vs)
	if err != nil {
		return 0, err
	}
	flagBuf, err := utils.IntToByteSlice(record.dateHeader.Flag)
	if err != nil {
		return 0, err
	}
	recordBuf := utils.AppendByteSlice(ksBuf, vsBuf, flagBuf, record.key, record.value)
	crc32  := utils.CreateCrc32(recordBuf)
	crcBuf, err := utils.UintToByteSlice(crc32)
	if err != nil {
		return 0, err
	}
	recordBuf = append(recordBuf, crcBuf...)
	curOffset, err := common.WriteFileWithByte(dataFile, offset, recordBuf)
	if err != nil {
		return 0, err
	}
	return curOffset, nil
}



