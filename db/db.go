package db

import (
	"akita/akerrors"
	"akita/common"
	"akita/consts"
	"akita/logger"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

// DB stands for the underlying storage
// providing a full memory index based on the map structure
// and sequential writing to data files
type DB struct {
	sync.Mutex
	dfPath string
	size   int64 // next insert offset
	iTable *indexTable
	// uses a buffered channel to pass write data,
	// a gr that writes data specifically reads recordBuffQueue and writes data to the db file.
	// this is design is to avoid using locks in I/O, "use communication to share data"
	recordBuffQueue chan []byte
	// write data errors are passed through recordBuffWriteErrs in current gr and write data gr
	recordBuffWriteErrs map[uint32]chan error
	recordBufPool       *common.BytePool
}

// OpenDB create a db object with data file path..
func OpenDB(fPath string) *DB {
	dir := filepath.Dir(fPath)
	ok, err := common.FileIsExit(dir)
	if err != nil {
		logger.Fatalf("get data dir is exit error: %s", err)
	}
	if !ok {
		if err = os.Mkdir(dir, os.ModePerm); err != nil {
			logger.Fatalf("make data file folder error: %s", err)
		}
	}

	dbFile, err := os.OpenFile(fPath, os.O_RDONLY, 0644)
	if err != nil {
		logger.Fatalf("open data file %s error: %v", fPath, err)
	}
	defer dbFile.Close()

	fi, err := dbFile.Stat()
	if err != nil {
		logger.Fatalf("get data file %s info error: %s", fPath, err)
	}

	db := &DB{
		dfPath:              fPath,
		size:                fi.Size(),
		iTable:              newIndexTable(),
		recordBuffQueue:     make(chan []byte, 100),
		recordBuffWriteErrs: make(map[uint32]chan error),
		recordBufPool:       common.NewBytePool(100, 2*consts.M),
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
	if length <= consts.LengthRecordHeader {
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
		ksBuf := dataBuff[bufOffset:(bufOffset + consts.LengthKs)]
		vsBuf := dataBuff[(bufOffset + consts.LengthKs):(bufOffset + consts.LengthKVs)]
		flagBuf := dataBuff[(bufOffset + consts.LengthKVs):(bufOffset + consts.LengthRecordHeader)]

		ks, err := common.ByteSliceToInt32(ksBuf)
		if err != nil {
			return err
		}
		vs, err := common.ByteSliceToInt32(vsBuf)
		if err != nil {
			return err
		}
		keyBuf := dataBuff[(bufOffset + consts.LengthRecordHeader):(bufOffset + consts.LengthRecordHeader + int64(ks))]
		key := common.ByteSliceToString(keyBuf)
		flag, err := common.ByteSliceToInt32(flagBuf)
		if err != nil {
			return err
		}
		if flag == consts.FlagDelete {
			db.iTable.remove(key)
			bufOffset += consts.LengthRecordHeader + int64(ks) + int64(vs)
			continue
		}

		rs := consts.LengthRecordHeader + int(ks) + int(vs) + consts.LengthCrc32
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
		logger.Errorf("read data from file error: %s", err)
		return nil, err
	}

	ksBuf := recordBuf[0:consts.LengthKs]
	ks, err := common.ByteSliceToInt32(ksBuf)
	if err != nil {
		logger.Errorf("turn byte slice to int32 error: %s", err)
		return nil, err
	}

	valueBuf := recordBuf[(consts.LengthRecordHeader + int64(ks)):(length - consts.LengthCrc32)]
	recordCrcBuf := recordBuf[(length - consts.LengthCrc32):length]
	recordCrc32, err := common.ByteSliceToUint(recordCrcBuf)
	if err != nil {
		logger.Errorf("turn byte slice to uint error: %s", err)
		return nil, err
	}

	crcSrcBuf := recordBuf[0:(length - consts.LengthCrc32)]
	crc32 := common.CreateCrc32(crcSrcBuf)
	if crc32 != recordCrc32 {
		logger.Warningf("the data which offset: %v, length: %v has been modified, not safe. ", offset, length)
		return nil, akerrors.ErrDataHasBeenModified
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
	if err = db.GetWriteRecordResult(recordBuf); err != nil {
		logger.Errorf("write record error: %v", err)
		return err
	}
	it := db.iTable
	ri := &recordIndex{offset: offsize, size: int64(len(recordBuf))}
	it.put(common.ByteSliceToString(record.key), ri)
	return nil
}

// WriteRecordNoCrc32 write byte stream record but no crc32 to data file.
func (db *DB) WriteRecordNoCrc32(record *dataRecord) error {
	rf, err := db.genRecordBuf(record, false)
	if err != nil {
		return err
	}
	db.PushRecordToQueue(rf)
	if err = db.GetWriteRecordResult(rf); err != nil {
		logger.Errorf("write record error: %v", err)
		return err
	}
	return nil
}

// GetDataByOffset get byte stream from data file at offset.
func (db *DB) GetDataByOffset(offset int64) ([]byte, error) {
	length := db.GetSyncSize() - offset
	if length <= 0 {
		return nil, akerrors.ErrNoDataUpdate
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
	ksBuff, err := common.Int32ToByteSlice(record.dateHeader.Ks)
	if err != nil {
		logger.Errorf("turn int32 to byte slice error: %s", err)
		return nil, err
	}
	vsBuff, err := common.Int32ToByteSlice(record.dateHeader.Vs)
	if err != nil {
		logger.Errorf("turn int32 to byte slice error: %s", err)
		return nil, err
	}
	flagBuff, err := common.Int32ToByteSlice(record.dateHeader.Flag)
	if err != nil {
		logger.Errorf("turn int32 to byte slice error: %s", err)
		return nil, err
	}
	recordBuff := db.recordBufPool.Get()
	recordBuff = append(recordBuff, ksBuff...)
	recordBuff = append(recordBuff, vsBuff...)
	recordBuff = append(recordBuff, flagBuff...)
	recordBuff = append(recordBuff, record.key...)
	recordBuff = append(recordBuff, record.value...)

	if checkCrc32 {
		crc32 := common.CreateCrc32(recordBuff)
		crc32Buff, err := common.UintToByteSlice(crc32)
		if err != nil {
			logger.Errorf("turn uint to byte slice error: %v", err)
			return nil, err
		}
		recordBuff = append(recordBuff, crc32Buff...)
	}

	return recordBuff, nil
}

// Close recycle some resource.
func (db *DB) Close() error {
	for key := range db.recordBuffWriteErrs {
		close(db.recordBuffWriteErrs[key])
	}
	close(db.recordBuffQueue)
	return nil
}

// WriteSyncData write byte stream data to data file.
func (db *DB) WriteSyncData(dataBuff []byte) error {
	offset := db.GetSyncSize()
	db.PushRecordToQueue(dataBuff)
	if err := db.GetWriteRecordResult(dataBuff); err != nil {
		logger.Errorf("write sync data error: %v", err)
		return err
	}

	err := db.UpdateTableWithData(offset, dataBuff)
	if err != nil {
		logger.Errorf("update index table error: %v", err)
		return err
	}
	return nil
}

// WriteRecordBuffQueueData write the data to data file with channel.
func (db *DB) WriteRecordBuffQueueData() {
	for {
		select {
		case r := <-db.recordBuffQueue:
			k := common.CreateCrc32(r)
			db.recordBuffWriteErrs[k] = make(chan error)
			dbFile, err := os.OpenFile(db.dfPath, os.O_WRONLY|os.O_APPEND, 0644)
			if err != nil {
				db.recordBuffWriteErrs[k] <- err
				continue
			}
			_, err = dbFile.Write(r)
			if err != nil {
				db.recordBuffWriteErrs[k] <- err
				dbFile.Close()
				continue
			}
			dbFile.Close()
			db.Lock()
			db.size += int64(len(r))
			db.Unlock()
			db.recordBuffWriteErrs[k] <- nil
			db.recordBufPool.Put(r)
		}
	}
}

// PushRecordToQueue send records to recordQueue
func (db *DB) PushRecordToQueue(r []byte) {
	db.recordBuffQueue <- r
}

// GetWriteRecordResult get write record error
func (db *DB) GetWriteRecordResult(r []byte) error {
	k := common.CreateCrc32(r)
	errCh, ok := db.recordBuffWriteErrs[k]
	if !ok {
		return errors.New("data has some problem")
	}
	defer delete(db.recordBuffWriteErrs, k)
	defer close(errCh)
	return <-errCh
}

// DataFileSync flush system buffer data to the data file.
func (db *DB) DataFileSync() {
	dbFile, err := os.OpenFile(db.dfPath, os.O_WRONLY, 0644)
	if err != nil {
		logger.Errorf("open data file error: %v", err)
		return
	}
	dbFile.Sync()
}
