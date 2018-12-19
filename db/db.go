package db

import (
	"akita/common"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// 数据文件对应结构体
type DB struct {
	lock     sync.Mutex  // 互斥锁
	dataFile *os.File    // 数据文件
	size     int64       // 记录文件大小/同时也可以当做文件下次索引位置
	iTable   *indexTable // 数据索引
}

// 在 OpenDB 函数中出错，直接退出程序
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

// 数据重新载入， 重建索引的过程
func (db *DB) Reload() error {
	// 根据文件大小判断文件是否需要重新
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

// 向数据文件中写入一条记录
func (db *DB) WriteRecord(record *dataRecord) (int64, error) {
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
	db.lock.Lock() // 互斥锁上锁
	recordLength, err := common.WriteBufToFile(db.dataFile, db.size, recordBuf)
	if err != nil {
		common.Error.Printf("Write data to file error: %s\n", err)
		return 0, err
	}
	db.size += recordLength
	defer db.lock.Unlock() // 解锁
	return recordLength, nil
}

func (db *DB) ReadRecord(offset int64, length int64) ([]byte, error) {
	recordBuf, err := common.ReadFileToByte(db.dataFile, offset, length)
	if err != nil {
		common.Error.Printf("Rear data from file error: %s\n", err)
		return nil, err
	}
	ksBuf := recordBuf[0:common.KsByteLength:common.KsByteLength]
	ks, err := common.ByteSliceToInt32(ksBuf)
	if err != nil {
		common.Error.Printf("Turn byte slice to int32 error: %s\n", err)
		return nil, err
	}
	valueBuf := recordBuf[(common.KvsByteLength + common.FlagByteLength + int64(ks) - 1):(length - common.CrcByteLength)]
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
