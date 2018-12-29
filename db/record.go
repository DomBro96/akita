package db

type (
	dataHeader struct {
		Ks   int32  // key size
		Vs   int32  // value size
		Crc  uint32 // crc value
		Flag int32  // flag of record type
	}

	dataRecord struct {
		dateHeader *dataHeader
		key        []byte	// key bytes
		value      []byte	// value bytes
	}
)
