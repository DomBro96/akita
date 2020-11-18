package db

import (
	"akita/common"
	"akita/logger"
)

type (
	dataHeader struct {
		Ks   int32 // key size
		Vs   int32 // value size
		Flag int32 // flag of record type
	}

	dataRecord struct {
		dateHeader *dataHeader
		key        []byte // key bytes
		value      []byte // value bytes
	}
)

// getRecordBuf get record bytes
func (record *dataRecord) getRecordBuf(checkCrc32 bool) ([]byte, error) {
	ksBuf, err := common.Int32ToByteSlice(record.dateHeader.Ks)
	if err != nil {
		logger.Error.Printf("Turn int32 to byte slice error: %s\n", err)
		return nil, err
	}
	vsBuf, err := common.Int32ToByteSlice(record.dateHeader.Vs)
	if err != nil {
		logger.Error.Printf("Turn int32 to byte slice error: %s\n", err)
		return nil, err
	}
	flagBuf, err := common.Int32ToByteSlice(record.dateHeader.Flag)
	if err != nil {
		logger.Error.Printf("Turn int32 to byte slice error: %s\n", err)
		return nil, err
	}
	recordBuf := common.AppendByteSlice(ksBuf, vsBuf, flagBuf, record.key, record.value)
	if checkCrc32 {
		crc32 := common.CreateCrc32(recordBuf)
		crcBuf, err := common.UintToByteSlice(crc32)
		if err != nil {
			logger.Error.Printf("Turn uint to byte slice error: %s\n", err)
			return nil, err
		}
		recordBuf = append(recordBuf, crcBuf...)
	}

	return recordBuf, nil
}
