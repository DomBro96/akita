package db

type (

	AkitaDb interface {
		Set(key string, value []byte) (bool, error)
		Get(key string) ([]byte, error)
		Del(key string) ([]byte, error)
	}

	HashDb struct {
		*DataHeader
		kb []byte
		vb []byte 	// value
	}

	DataHeader struct {
		ks int32 	// key size
		vs int32 	// value size
		crc int32 	// judge the file is
		flag int32 	// type of write to file
	}

)


func (*HashDb) Set(key string, value []byte) (error){

	return nil
}

func (*HashDb) Get(key string) ([]byte, error) {
	return nil, nil
}

func (*HashDb) Del(key string) ([]byte, error) {
	return nil, nil
}
