package db

type (
	DataHeader struct {
		Ks   int32 // key size
		Vs   int32 // value size
		Crc  int32 // judge the file is
		Flag int32 // type of write to file
	}

	// 表示写入数据库文件的数据结构
	AkitaRecord struct {
		*DataHeader
		key   []byte
		Value []byte
	}
)
