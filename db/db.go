package db

import (
	"akita/common"
	"os"
	"path/filepath"
	"strconv"
	"sync"
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

func (db *DB) Reload() error { // reload database index table
	var length int64
	db.lock.Lock()
	if length = db.size; length < common.KvsByteLength+common.FlagByteLength+common.CrcByteLength {
		return nil
	}
	db.lock.Unlock()
	var offset int64 = 0
	return db.UpdateTable(offset, length)
}

func (db *DB) UpdateTable(offset int64, length int64) error {
	dataBuf, err := common.ReadFileToByte(db.dataFile, offset, length)
	if err != nil {
		return err
	}
	for offset < length {
		ksBuf := dataBuf[offset:(offset + common.KsByteLength)]
		vsBuf := dataBuf[(offset + common.KsByteLength):(offset + common.KvsByteLength)]
		flagBuf := dataBuf[(offset + common.KvsByteLength):(offset + common.KvsByteLength + common.FlagByteLength)]

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
		keyBuf := dataBuf[(offset + common.KvsByteLength + common.FlagByteLength):(offset + common.KvsByteLength + common.FlagByteLength + int64(ks))]
		key := common.ByteSliceToString(keyBuf)
		rs := common.KvsByteLength + common.FlagByteLength + int(ks) + int(vs)
		if flag == common.DeleteFlag {
			db.iTable.remove(key)
		} else {
			rs += common.CrcByteLength
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

func (db *DB) WriteRecord(record *dataRecord) (int64, error) { // write a record to data file
	recordBuf, err := record.getRecordBuf()
	if err != nil {
		return 0, err
	}
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
	db.size += recordLength
	defer db.lock.Unlock()
	return recordLength, nil
}

func (db *DB) WriteRecordNoCrc32(record *dataRecord) (int64, error) {
	recordBuf, err := record.getRecordBuf()
	if err != nil {
		return 0, err
	}
	db.lock.Lock()
	recordLength, err := common.WriteBufToFile(db.dataFile, db.size, recordBuf)
	if err != nil {
		common.Error.Printf("Write data to file error: %s\n", err)
		return 0, err
	}
	db.size += recordLength
	db.lock.Unlock()
	return recordLength, nil
}

func (db *DB) GetDataByOffset(offset int64) ([]byte, error) {
	db.lock.Lock()
	length := db.size - offset
	db.lock.Unlock()
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
		common.Error.Printf("data file close err: %s\n", err)
		return err
	}
	return nil
}

func (db *DB) WriteSyncData(dataBuf []byte) error {
	var offset int64
	db.lock.Lock()
	offset = db.size
	length, err := common.WriteBufToFile(db.dataFile, offset, dataBuf)
	if err != nil {
		common.Error.Printf("write sync data error: %s\n", err)
		return err
	}
	err = db.UpdateTable(offset, length)
	if err != nil {
		common.Error.Printf("update index table error: %s\n", err)
		return err
	}
	db.size += length
	db.lock.Unlock()
	return nil
}
