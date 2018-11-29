package db

type (

	AkitaDb interface {
		Set(key []byte, value []byte) (bool, error)
		Get(key []byte) ([]byte, error)
		Del(key []byte) ([]byte, error)
	}

	HashDb struct {
		*DataHeader
		key []byte
		Value []byte 	// value
	}

	DataHeader struct {
		Ks int32 	// key size
		Vs int32 	// value size
		Crc int32 	// judge the file is
		Flag int32 	// type of write to file
	}

)

func (db *HashDb) Set(key []byte, value []byte) (error){
	return nil
}

func (db *HashDb) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (db *HashDb)Del(key []byte) ([]byte, error) {
	return nil, nil
}


