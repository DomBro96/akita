package db

import (
	"akita/common"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
)

type DB struct {
	lock     sync.Mutex
	dataFile *os.File    // data file
	size     int64       // data file size / next insert offset
	iTable   *indexTable // database index
}

func OpenDB(path string) *DB {
	dir := filepath.Dir(path)
	ok, err := common.FileIsExit(dir)
	if err != nil {
		common.Error.Fatalf("Get data dir is exit error: %s\n", err)
		return nil
	}
	if !ok {
		err = os.Mkdir(dir, os.ModePerm)
		if err != nil {
			common.Error.Fatalf("Make data file folder error: %s\n", err)
			return nil
		}
	}
	dbFile, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		common.Error.Fatalf("Open data file error: %s\n", err)
		return nil
	}
	fs, err := common.GetFileSize(dbFile)
	if err != nil {
		common.Error.Fatalf("Get data file size error: %s\n", err)
		return nil
	}
	db := &DB{
		dataFile: dbFile,
		size:     fs,
		iTable:   newIndexTable(),
	}
	return db
}


func (db *DB) Reload() error {		// reload database index table
	if db.size < common.KvsByteLength+common.FlagByteLength+common.CrcByteLength {
		return nil
	}
	var offset int64 = 0
	dataBuff, err := common.ReadFileToByte(db.dataFile, offset, db.size)
	if err != nil {
		return err
	}
	for offset < db.size {
		ksBuf := dataBuff[offset:(offset + common.KsByteLength)]
		vsBuf := dataBuff[(offset + common.KsByteLength):(offset + common.KvsByteLength)]
		flagBuf := dataBuff[(offset + common.KvsByteLength):(offset + common.KvsByteLength + common.FlagByteLength)]
		ks, err := common.ByteSliceToInt32(ksBuf)
		if err != nil {
			return err
		}
		vs, err := common.ByteSliceToInt32(vsBuf)
		if err != nil {
			return err
		}
		flag, err := common.ByteSliceToInt32(flagBuf)
		if err != nil {
			return err
		}
		keyBuf := dataBuff[(offset + common.KvsByteLength + common.FlagByteLength):(offset + common.KvsByteLength + common.FlagByteLength + int64(ks))]
		key := common.ByteSliceToString(keyBuf)
		rs := common.KvsByteLength + common.FlagByteLength + common.CrcByteLength + int(ks) + int(vs)
		if flag == common.DeleteFlag {
			db.iTable.remove(key)
		} else {
			ri := recordIndex{
				offset: offset,
				size:   rs,
			}
			db.iTable.put(key, &ri)
		}
		offset += int64(rs)
	}
	return nil
}


func (db *DB) WriteRecord(record *dataRecord) (int64, error) {	// write a record to data file
	ksBuf, err := common.Int32ToByteSlice(record.dateHeader.Ks)
	if err != nil {
		common.Error.Printf("Turn int32 to byte slice error: %s\n", err)
		return 0, err
	}
	vsBuf, err := common.Int32ToByteSlice(record.dateHeader.Vs)
	if err != nil {
		common.Error.Printf("Turn int32 to byte slice error: %s\n", err)
		return 0, err
	}
	flagBuf, err := common.Int32ToByteSlice(record.dateHeader.Flag)
	if err != nil {
		common.Error.Printf("Turn int32 to byte slice error: %s\n", err)
		return 0, err
	}
	recordBuf := common.AppendByteSlice(ksBuf, vsBuf, flagBuf, record.key, record.value)
	crc32 := common.CreateCrc32(recordBuf)
	crcBuf, err := common.UintToByteSlice(crc32)
	if err != nil {
		common.Error.Printf("Turn uint to byte slice error: %s\n", err)
		return 0, err
	}
	recordBuf = append(recordBuf, crcBuf...)
	db.lock.Lock()
	recordLength, err := common.WriteBufToFile(db.dataFile, db.size, recordBuf)
	if err != nil {
		common.Error.Printf("Write data to file error: %s\n", err)
		return 0, err
	}
	defer db.lock.Unlock()
	atomic.AddInt64(&db.size, recordLength)
	return recordLength, nil
}

func (db *DB) ReadRecord(offset int64, length int64) ([]byte, error) {
	recordBuf, err := common.ReadFileToByte(db.dataFile, offset, length)
	if err != nil {
		common.Error.Printf("Rear data from file error: %s\n", err)
		return nil, err
	}
	ksBuf := recordBuf[0:common.KsByteLength]
	ks, err := common.ByteSliceToInt32(ksBuf)
	if err != nil {
		common.Error.Printf("Turn byte slice to int32 error: %s\n", err)
		return nil, err
	}
	valueBuf := recordBuf[(common.KvsByteLength + common.FlagByteLength + int64(ks)):(length - common.CrcByteLength)]
	crcSrcBuf := recordBuf[0:(length - common.CrcByteLength)]
	recordCrcBuf := recordBuf[(length - common.CrcByteLength):length]
	checkCrc32, err := common.ByteSliceToUint(recordCrcBuf)
	if err != nil {
		common.Error.Printf("Turn byte slice to uint error: %s\n", err)
		return nil, err
	}
	crc32 := common.CreateCrc32(crcSrcBuf)
	if crc32 != checkCrc32 {
		common.Warning.Printf("The data which offset is " + strconv.Itoa(int(offset)) + " has been modified, not safe. ")
		return nil, common.ErrDataHasBeenModified
	}
	return valueBuf, nil
}

func (db *DB) getDataByOffset(offset int64) ([]byte, error) {
	length := atomic.LoadInt64(&db.size) - offset
	if length == 0 {
		return nil, common.ErrNoDataUpdate
	}
	data, err := common.ReadFileToByte(db.dataFile, offset, length)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (db *DB) Close() error  {
	err := db.dataFile.Close()
	if err != nil {
		common.Error.Printf("Data file close err: %s\n", err)
		return err
	}
	return nil
}