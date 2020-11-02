package db

import (
	"akita/common"
	"akita/logger"
	"os"
	"path/filepath"
	"sync"
)

// DB kv database struct for akita.
type DB struct {
	sync.Mutex               // todo: should use block channel instead of lock?
	dataFilePath string      // data file path
	size         int64       // data file size / next insert offset
	iTable       *indexTable // database index
}

// OpenDB New Object.
func OpenDB(fPath string) *DB {
	dir := filepath.Dir(fPath)
	ok, err := common.FileIsExit(dir)
	if err != nil {
		logger.Error.Fatalf("Get data dir is exit error: %s\n", err)
	}
	if !ok {
		if err = os.Mkdir(dir, os.ModePerm); err != nil {
			logger.Error.Fatalf("Make data file folder error: %s\n", err)
		}
	}

	// get dbFile size, and reload index
	dbFile, err := os.OpenFile(fPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		logger.Error.Fatalf("Open data file "+fPath+" error: %s\n", err)
	}
	defer dbFile.Close()

	fs, err := common.GetFileSize(dbFile)
	if err != nil {
		logger.Error.Fatalf("Get data file size error: %s\n", err)
	}
	db := &DB{
		dataFilePath: fPath,
		size:         fs,
		iTable:       newIndexTable(),
	}
	return db
}

func (db *DB) GetSize() int64 {
	db.Lock()
	defer db.Unlock()
	return db.size
}

// Reload reload database index table.
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

// UpdateTable update db index table.
func (db *DB) UpdateTable(offset int64, length int64) error {

	dbFile, err := os.OpenFile(db.dataFilePath, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer dbFile.Close()

	dataBuf, err := common.ReadFileToByte(dbFile, offset, length)
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
			offset: offset + bufOffset,
			size:   rs,
		}
		db.iTable.put(key, &ri)
		bufOffset += int64(rs)
	}
	return nil
}

// ReadRecord read data to memery.
func (db *DB) ReadRecord(offset int64, length int64) ([]byte, error) {
	dbFile, err := os.OpenFile(db.dataFilePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer dbFile.Close()

	recordBuf, err := common.ReadFileToByte(dbFile, offset, length)
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

// WriteRecord write a record to data file.
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
	dbFile, err := os.OpenFile(db.dataFilePath, os.O_WRONLY, 0644)
	if err != nil {
		return -1, err
	}
	defer dbFile.Close()

	recordLength, err := common.WriteBufToFile(dbFile, db.size, recordBuf)
	if err != nil {
		logger.Error.Printf("Write data to file error: %s\n", err)
		return 0, err
	}
	db.size += recordLength
	return recordLength, nil
}

// WriteRecordNoCrc32 write a record to data file with no crc32.
func (db *DB) WriteRecordNoCrc32(record *dataRecord) (int64, error) {
	recordBuf, err := record.getRecordBuf()
	if err != nil {
		return 0, err
	}
	db.Lock()
	defer db.Unlock()
	dbFile, err := os.OpenFile(db.dataFilePath, os.O_WRONLY, 0644)
	if err != nil {
		return -1, err
	}
	defer dbFile.Close()

	recordLength, err := common.WriteBufToFile(dbFile, db.size, recordBuf)
	if err != nil {
		logger.Error.Printf("Write data to file error: %s\n", err)
		return 0, err
	}
	db.size += recordLength
	return recordLength, nil
}

// GetDataByOffset get data from db file offset.
func (db *DB) GetDataByOffset(offset int64) ([]byte, error) {
	length := db.GetSize() - offset
	if length <= 0 {
		return nil, common.ErrNoDataUpdate
	}
	dbFile, err := os.OpenFile(db.dataFilePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer dbFile.Close()

	data, err := common.ReadFileToByte(dbFile, offset, length)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Close recycle some resource.
func (db *DB) Close() error {
	return nil
}

// WriteSyncData salve server write sync data.
func (db *DB) WriteSyncData(dataBuf []byte) error {
	var offset int64
	db.Lock()
	defer db.Unlock()
	offset = db.size
	length, err := common.WriteBufToFile(db.dataFilePath, offset, dataBuf)
	if err != nil {
		logger.Error.Printf("write sync data error: %s\n", err)
		return err
	}
	db.size += length

	err = db.UpdateTable(offset, length)
	if err != nil {
		logger.Error.Printf("update index table error: %s\n", err)
		return err
	}
	return nil
}
