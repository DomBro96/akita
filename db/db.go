package db

import (
	"akita/aerrors"
	"akita/common"
	"akita/logger"
	"os"
	"path/filepath"
	"sync"
)

// DB kv database struct.
type DB struct {
	sync.Mutex
	dfPath          string       // data file path
	size            int64        // data file size / next insert offset
	iTable          *indexTable  // db index
	recordQueue     chan []byte  // recordQueue uses channel to write data to db file avoid using lock in I/O, "use communication to share data"
	recordWriteErrs []chan error // recordWriteErrs passing write record error
	rwErrIndex      int          // rwErrIndex index of rec
	recordBufPool   *common.BytePool
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
		dfPath:          fPath,
		size:            fs,
		iTable:          newIndexTable(),
		recordQueue:     make(chan []byte, 100),
		recordWriteErrs: make([]chan error, 100),
		rwErrIndex:      0,
		recordBufPool:   common.NewBytePool(100, 2*common.M),
	}
	for i := range db.recordWriteErrs {
		db.recordWriteErrs[i] = make(chan error)
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

	dataBuff, err := common.ReadFileToBytes(dbFile, offset, length)
	if err != nil {
		return err
	}
	return db.UpdateTableWithData(offset, dataBuff)
}

// UpdateTableWithData update db index table with data buf.
func (db *DB) UpdateTableWithData(offset int64, dataBuff []byte) error {

	bufOffset, length := int64(0), int64(len(dataBuff))
	for bufOffset < length {
		ksBuf := dataBuff[bufOffset:(bufOffset + common.KsByteLength)]
		vsBuf := dataBuff[(bufOffset + common.KsByteLength):(bufOffset + common.KvsByteLength)]
		flagBuf := dataBuff[(bufOffset + common.KvsByteLength):(bufOffset + common.RecordHeaderByteLength)]

		ks, err := common.ByteSliceToInt32(ksBuf)
		if err != nil {
			return err
		}
		vs, err := common.ByteSliceToInt32(vsBuf)
		if err != nil {
			return err
		}
		keyBuf := dataBuff[(bufOffset + common.RecordHeaderByteLength):(bufOffset + common.RecordHeaderByteLength + int64(ks))]
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
		return nil, aerrors.ErrDataHasBeenModified
	}
	return valueBuf, nil
}

// WriteRecord write byte stream record to data file.
func (db *DB) WriteRecord(record *dataRecord) error {
	recordBuf, err := db.genRecordBuf(record, true)
	if err != nil {
		return err
	}
	offsize := db.GetSyncSize()
	db.PushRecordToQueue(recordBuf)
	if err = db.GetWriteRecordResult(); err != nil {
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
	recordBuf, err := db.genRecordBuf(record, false)
	if err != nil {
		return err
	}
	db.PushRecordToQueue(recordBuf)
	if err = db.GetWriteRecordResult(); err != nil {
		logger.Error.Printf("write record error: %v. \n", err)
		return err
	}
	return nil
}

// GetDataByOffset get byte stream from data file at offset.
func (db *DB) GetDataByOffset(offset int64) ([]byte, error) {
	length := db.GetSyncSize() - offset
	if length <= 0 {
		return nil, aerrors.ErrNoDataUpdate
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

func (db *DB) genRecordBuf(record *dataRecord, checkCrc32 bool) ([]byte, error) {
	ksBuf, err := common.Int32ToByteSlice(record.dateHeader.Ks)
	if err != nil {
		logger.Error.Printf("Turn int32 to byte slice error: %s\n", err)
		return nil, err
	}
	vsBuf, err := common.Int32ToByteSlice(record.dateHeader.Vs)
	if err != nil {
		logger.Error.Printf("Turn int32 to byte slice error: %s\n", err)
		return nil, err
	}
	flagBuf, err := common.Int32ToByteSlice(record.dateHeader.Flag)
	if err != nil {
		logger.Error.Printf("Turn int32 to byte slice error: %s\n", err)
		return nil, err
	}
	recordBuf := db.recordBufPool.Get()
	recordBuf = append(recordBuf, ksBuf...)
	recordBuf = append(recordBuf, vsBuf...)
	recordBuf = append(recordBuf, flagBuf...)
	recordBuf = append(recordBuf, record.key...)
	recordBuf = append(recordBuf, record.value...)
	if checkCrc32 {
		crc32 := common.CreateCrc32(recordBuf)
		crcBuf, err := common.UintToByteSlice(crc32)
		if err != nil {
			logger.Error.Printf("Turn uint to byte slice error: %s\n", err)
			return nil, err
		}
		recordBuf = append(recordBuf, crcBuf...)
	}

	return recordBuf, nil
}

// Close recycle some resource.
func (db *DB) Close() error {
	for i := range db.recordWriteErrs {
		close(db.recordWriteErrs[i])
	}
	close(db.recordQueue)
	return nil
}

// WriteSyncData write byte stream data to data file.
func (db *DB) WriteSyncData(dataBuff []byte) error {
	offset := db.GetSyncSize()
	db.PushRecordToQueue(dataBuff)
	if err := db.GetWriteRecordResult(); err != nil {
		logger.Error.Printf("write sync data error: %v. \n", err)
		return err
	}

	err := db.UpdateTableWithData(offset, dataBuff)
	if err != nil {
		logger.Error.Printf("update index table error: %s\n", err)
		return err
	}
	return nil
}

// WriteFromRecordQueue write the data to data file with channel.
func (db *DB) WriteFromRecordQueue() {

	for {
		select {
		case r := <-db.recordQueue:
			i := db.rwErrIndex
			dbFile, err := os.OpenFile(db.dfPath, os.O_WRONLY, 0644)
			if err != nil {
				db.recordWriteErrs[i] <- err
				continue
			}
			recordLength, err := common.WriteBufToFile(dbFile, db.size, r)
			if err != nil {
				db.recordWriteErrs[i] <- err
				dbFile.Close()
				continue
			}
			dbFile.Close()
			db.Lock()
			db.size += recordLength
			db.Unlock()
			db.recordWriteErrs[i] <- nil
			db.recordBufPool.Put(r)
		}
	}
}

// PushRecordToQueue send records to recordQueue
func (db *DB) PushRecordToQueue(r []byte) {
	db.recordQueue <- r
}

// GetWriteRecordResult get write record error
func (db *DB) GetWriteRecordResult() error {
	i := db.rwErrIndex
	err := <-db.recordWriteErrs[i]
	if db.rwErrIndex < len(db.recordWriteErrs)-1 {
		db.rwErrIndex++
	} else {
		db.rwErrIndex = 0
	}
	return err
}

// DataFileSync flush system buffer data to the data file.
func (db *DB) DataFileSync() {
	dbFile, err := os.OpenFile(db.dfPath, os.O_WRONLY, 0644)
	if err != nil {
		logger.Error.Printf("open data file error: %v", err)
		return
	}
	dbFile.Sync()
}
