package db

type (
	dataHeader struct {
		Ks   int32 // key size
		Vs   int32 // value size
		Flag int32 // flag of record type
	}

	dataRecord struct {
		header *dataHeader
		key    []byte // key bytes
		value  []byte // value bytes
	}
)
