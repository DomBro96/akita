package db

type (
	DataHeader struct {
		Ks   int32  // key size
		Vs   int32  // value size
		Crc  uint32 // judge the file is
		Flag int32  // type of write to file
	}

	// 写入数据库文件的数据结构
	DataRecord struct {
		dateHeader *DataHeader
		key        []byte
		value      []byte
	}
)
