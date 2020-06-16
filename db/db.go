package db

import (
	"akita/common"
	"akita/logger"
	"os"
	"path/filepath"
	"sync"
)

type DB struct {
	sync.Mutex
	dataFile *os.File    // data file
	size     int64       // data file size / next insert offset
	iTable   *indexTable // database index
}

func OpenDB(path string) *DB {
	dir := filepath.Dir(path)
	ok, err := common.FileIsExit(dir)
	if err != nil {
		logger.Error.Fatalf("Get data dir is exit error: %s\n", err)
		return nil
	}
	if !ok {
		err = os.Mkdir(dir, os.ModePerm)
		if err != nil {
			logger.Error.Fatalf("Make data file folder error: %s\n", err)
			return nil
		}
	}
	dbFile, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		logger.Error.Fatalf("Open data file "+path+" error: %s\n", err)
		return nil
	}
	fs, err := common.GetFileSize(dbFile)
	if err != nil {
		logger.Error.Fatalf("Get data file size error: %s\n", err)
		return nil
	}
	db := &DB{
		dataFile: dbFile,
		size:     fs,
		iTable:   newIndexTable(),
	}
	return db
}

// reload database index table
func (db *DB) Reload() error {
	db.Lock()
	length := db.size
	db.Unlock()
	if length <= common.RecordHeaderByteLength {
		return nil
	}

	complete := make(chan error)
	go func() {
		// will block
		complete <- db.UpdateTable(0, length)
	}()

	return <-complete
}

func (db *DB) UpdateTable(offset int64, length int64) error {
	dataBuf, err := common.ReadFileToByte(db.dataFile, offset, length)
	if err != nil {
		return err
	}
	bufOffset := int64(0)
	for bufOffset < length {
		ksBuf := dataBuf[bufOffset:(bufOffset + common.KsByteLength)]
		vsBuf := dataBuf[(bufOffset + common.KsByteLength):(bufOffset + common.KvsByteLength)]
		flagBuf := dataBuf[(bufOffset + common.KvsByteLength):(bufOffset + common.RecordHeaderByteLength)]

		ks, err := common.ByteSliceToInt32(ksBuf)
		if err != nil {
			return err
		}
		vs, err := common.ByteSliceToInt32(vsBuf)
		if err != nil {
			return err
		}
		keyBuf := dataBuf[(bufOffset + common.RecordHeaderByteLength):(bufOffset + common.RecordHeaderByteLength + int64(ks))]
		key := common.ByteSliceToString(keyBuf)
		flag, err := common.ByteSliceToInt32(flagBuf)
		if err != nil {
			return err
		}
		if flag == common.DeleteFlag {
			db.iTable.remove(key)
			continue
		}

		rs := common.RecordHeaderByteLength + int(ks) + int(vs) + common.CrcByteLength
		ri := recordIndex{
			offset: offset+bufOffset,
			size:   rs,
		}
		db.iTable.put(key, &ri)
		bufOffset += int64(rs)
	}
	return nil
}

func (db *DB) ReadRecord(offset int64, length int64) ([]byte, error) {
	recordBuf, err := common.ReadFileToByte(db.dataFile, offset, length)
	if err != nil {
		logger.Error.Printf("Read data from file error: %s\n", err)
		return nil, err
	}
	ksBuf := recordBuf[0:common.KsByteLength]
	ks, err := common.ByteSliceToInt32(ksBuf)
	if err != nil {
		logger.Error.Printf("Turn byte slice to int32 error: %s\n", err)
		return nil, err
	}

	valueBuf := recordBuf[(common.RecordHeaderByteLength + int64(ks)):(length - common.CrcByteLength)]
	recordCrcBuf := recordBuf[(length - common.CrcByteLength):length]
	recordCrc32, err := common.ByteSliceToUint(recordCrcBuf)
	if err != nil {
		logger.Error.Printf("Turn byte slice to uint error: %s\n", err)
		return nil, err
	}

	crcSrcBuf := recordBuf[0:(length - common.CrcByteLength)]
	crc32 := common.CreateCrc32(crcSrcBuf)
	if crc32 != recordCrc32 {
		logger.Warning.Printf("The data which offset: %v, length: %v has been modified, not safe. ", offset, length)
		return nil, common.ErrDataHasBeenModified
	}
	return valueBuf, nil
}

// write a record to data file
func (db *DB) WriteRecord(record *dataRecord) (int64, error) {
	recordBuf, err := record.getRecordBuf()
	if err != nil {
		return 0, err
	}
	crc32 := common.CreateCrc32(recordBuf)
	crcBuf, err := common.UintToByteSlice(crc32)
	if err != nil {
		logger.Error.Printf("Turn uint to byte slice error: %s\n", err)
		return 0, err
	}
	recordBuf = append(recordBuf, crcBuf...)
	db.Lock()
	defer db.Unlock()
	recordLength, err := common.WriteBufToFile(db.dataFile, db.size, recordBuf)
	if err != nil {
		logger.Error.Printf("Write data to file error: %s\n", err)
		return 0, err
	}
	db.size += recordLength
	return recordLength, nil
}

func (db *DB) WriteRecordNoCrc32(record *dataRecord) (int64, error) {
	recordBuf, err := record.getRecordBuf()
	if err != nil {
		return 0, err
	}
	db.Lock()
	defer db.Unlock()
	recordLength, err := common.WriteBufToFile(db.dataFile, db.size, recordBuf)
	if err != nil {
		logger.Error.Printf("Write data to file error: %s\n", err)
		return 0, err
	}
	db.size += recordLength
	return recordLength, nil
}

func (db *DB) GetDataByOffset(offset int64) ([]byte, error) {
	db.Lock()
	length := db.size - offset
	db.Unlock()
	if length <= 0 {
		return nil, common.ErrNoDataUpdate
	}
	data, err := common.ReadFileToByte(db.dataFile, offset, length)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (db *DB) Close() error {
	err := db.dataFile.Close()
	if err != nil {
		return err
	}
	return nil
}

// salve server write sync data
func (db *DB) WriteSyncData(dataBuf []byte) error {
	var offset int64
	db.Lock()
	offset = db.size
	length, err := common.WriteBufToFile(db.dataFile, offset, dataBuf)
	if err != nil {
		logger.Error.Printf("write sync data error: %s\n", err)
		return err
	}
	db.size += length
	db.Unlock()
	err = db.UpdateTable(offset, length)
	if err != nil {
		logger.Error.Printf("update index table error: %s\n", err)
		return err
	}
	return nil
}
