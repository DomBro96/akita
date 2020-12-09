package db

import (
	"akita/common"
	"akita/logger"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// DB kv database struct.
type DB struct {
	sync.Mutex                           // todo: should use block channel instead of lock?
	dfPath         string                // data file path
	size           int64                 // data file size / next insert offset
	iTable         *indexTable           // db index
	rPipeline      chan []byte           // rPipeline uses channel to write data to db file avoid using lock in I/O, "use communication to share data"
	rWriteComplete map[string]chan error // rWriteComplete passing write record error
}

// OpenDB create a db object with data file path..
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
		dfPath:         fPath,
		size:           fs,
		iTable:         newIndexTable(),
		rPipeline:      make(chan []byte, 100),
		rWriteComplete: make(map[string]chan error),
	}
	return db
}

// GetSyncSize get data file size with lock.
func (db *DB) GetSyncSize() int64 {
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

// UpdateTable update db index table from data file.
func (db *DB) UpdateTable(offset int64, length int64) error {

	dbFile, err := os.OpenFile(db.dfPath, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer dbFile.Close()

	dataBuf, err := common.ReadFileToBytes(dbFile, offset, length)
	if err != nil {
		return err
	}
	return db.UpdateTableWithData(offset, dataBuf)
}

// UpdateTableWithData update db index table with data buf.
func (db *DB) UpdateTableWithData(offset int64, dataBuf []byte) error {

	bufOffset, length := int64(0), int64(len(dataBuf))
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
			bufOffset += common.RecordHeaderByteLength + int64(ks) + int64(vs)
			continue
		}

		rs := common.RecordHeaderByteLength + int(ks) + int(vs) + common.CrcByteLength
		ri := recordIndex{
			offset: offset + bufOffset,
			size:   int64(rs),
		}

		db.iTable.put(key, &ri)
		bufOffset += int64(rs)
	}
	return nil
}

// ReadRecord read data to memery.
func (db *DB) ReadRecord(offset int64, length int64) ([]byte, error) {
	dbFile, err := os.OpenFile(db.dfPath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer dbFile.Close()

	recordBuf, err := common.ReadFileToBytes(dbFile, offset, length)
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

// WriteRecord write byte stream record to data file.
func (db *DB) WriteRecord(record *dataRecord) error {
	recordBuf, err := record.getRecordBuf(true)
	if err != nil {
		return err
	}
	offsize := db.GetSyncSize()
	db.SendRecordToRPipline(recordBuf)
	if err = db.GetWriteRecordResult(recordBuf); err != nil {
		logger.Error.Printf("write record error: %v. \n", err)
		return err
	}
	it := db.iTable
	ri := &recordIndex{offset: offsize, size: int64(len(recordBuf))}
	it.put(string(record.key), ri)
	return nil
}

// WriteRecordNoCrc32 write byte stream record but no crc32 to data file.
func (db *DB) WriteRecordNoCrc32(record *dataRecord) error {
	recordBuf, err := record.getRecordBuf(false)
	if err != nil {
		return err
	}
	db.SendRecordToRPipline(recordBuf)
	if err = db.GetWriteRecordResult(recordBuf); err != nil {
		logger.Error.Printf("write record error: %v. \n", err)
		return err
	}
	return nil
}

// GetDataByOffset get byte stream from data file at offset.
func (db *DB) GetDataByOffset(offset int64) ([]byte, error) {
	length := db.GetSyncSize() - offset
	if length <= 0 {
		return nil, common.ErrNoDataUpdate
	}
	dbFile, err := os.OpenFile(db.dfPath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer dbFile.Close()

	data, err := common.ReadFileToBytes(dbFile, offset, length)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Close recycle some resource.
func (db *DB) Close() error {

	close(db.rPipeline)
	for k := range db.rWriteComplete {
		close(db.rWriteComplete[k])
		delete(db.rWriteComplete, k)
	}
	return nil
}

// WriteSyncData write byte stream data to data file.
func (db *DB) WriteSyncData(dataBuf []byte) error {
	offset := db.GetSyncSize()
	db.SendRecordToRPipline(dataBuf)
	if err := db.GetWriteRecordResult(dataBuf); err != nil {
		logger.Error.Printf("write sync data error: %v. \n", err)
		return err
	}

	err := db.UpdateTableWithData(offset, dataBuf)
	if err != nil {
		logger.Error.Printf("update index table error: %s\n", err)
		return err
	}
	return nil
}

// WriteRecordFromRPipline write the data to data file with channel.
func (db *DB) WriteRecordFromRPipline() {

	for {
		select {
		case r := <-db.rPipeline:
			rwcKey := db.generateRwcKey(r)
			db.rWriteComplete[rwcKey] = make(chan error)
			dbFile, err := os.OpenFile(db.dfPath, os.O_WRONLY, 0644)
			if err != nil {
				db.rWriteComplete[rwcKey] <- err
				continue
			}
			recordLength, err := common.WriteBufToFile(dbFile, db.size, r)
			if err != nil {
				db.rWriteComplete[rwcKey] <- err
				continue
			}
			dbFile.Close()
			db.Lock()
			db.size += recordLength
			db.Unlock()
			db.rWriteComplete[rwcKey] <- nil
		}
	}
}

// SendRecordToRPipline send records to rPipline
func (db *DB) SendRecordToRPipline(r []byte) {
	db.rPipeline <- r
}

// GetWriteRecordResult get write record error
func (db *DB) GetWriteRecordResult(rf []byte) error {
	rwcKey := db.generateRwcKey(rf)
	defer delete(db.rWriteComplete, rwcKey)
	return <-db.rWriteComplete[rwcKey]
}

func (db *DB) generateRwcKey(rf []byte) string {
	return fmt.Sprintf("rwc:len:%d:val:%d", len(rf), common.CreateCrc32(rf))
}
