package db

import "akita/common"

type (
	dataHeader struct {
		Ks   int32  // key size
		Vs   int32  // value size
		Crc  uint32 // crc value
		Flag int32  // flag of record type
	}

	dataRecord struct {
		dateHeader *dataHeader
		key        []byte // key bytes
		value      []byte // value bytes
	}
)

func (record *dataRecord) getRecordBuf() ([]byte, error) {
	ksBuf, err := common.Int32ToByteSlice(record.dateHeader.Ks)
	if err != nil {
		common.Error.Printf("Turn int32 to byte slice error: %s\n", err)
		return nil, err
	}
	vsBuf, err := common.Int32ToByteSlice(record.dateHeader.Vs)
	if err != nil {
		common.Error.Printf("Turn int32 to byte slice error: %s\n", err)
		return nil, err
	}
	flagBuf, err := common.Int32ToByteSlice(record.dateHeader.Flag)
	if err != nil {
		common.Error.Printf("Turn int32 to byte slice error: %s\n", err)
		return nil, err
	}
	recordBuf := common.AppendByteSlice(ksBuf, vsBuf, flagBuf, record.key, record.value)
	return recordBuf, nil
}
