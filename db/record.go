package db

type (
	DataHeader struct {
		Ks       int32 // key size
		Vs       int32 // value size
		Flag     int32 // flag of record type
		expireAt int64 // mark expire time
	}

	DataRecord struct {
		header *DataHeader
		key    []byte // key bytes
		value  []byte // value bytes
	}
)
