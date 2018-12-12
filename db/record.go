package db

type (
	dataHeader struct {
		Ks   int32  // key 大小
		Vs   int32  // value 大小
		Crc  uint32 // crc 数值
		Flag int32  // 记录标识类型
	}

	// 写入数据库文件的数据结构
	dataRecord struct {
		dateHeader *dataHeader
		key        []byte
		value      []byte
	}
)
