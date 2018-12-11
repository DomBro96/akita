package db

import (
	"akita/common"
	"sync"
)

// 数据文件对应结构体
type AkitaDB struct {
	mutex 		sync.Mutex			// 互斥锁
	dataFile    string			    // 数据文件
	size        int64				// 记录文件大小/同时也可以当做文件下次索引位置
}

var (
	dbInstance      *AkitaDB
	dbInstanceMutex sync.Mutex
	)



// 全局单例的 AkitaDB
func getSingletonAkitaDb() *AkitaDB{
	if dbInstance == nil {
		dbInstanceMutex.Lock()
		{
			dbInstance = &AkitaDB{dataFile: common.DefaultDataFile, size: 0}
		}
		dbInstanceMutex.Unlock()
	}
	return dbInstance
}

func (db *AkitaDB) Reload() (bool, error) { 											// 数据重新载入
	return false, nil
}


// 向数据文件中写入一条记录
func (db *AkitaDB)WriteRecord (record *DataRecord) (int64, error) {						// 将记录写入
	ksBuf, err := common.Int32ToByteSlice(record.dateHeader.Ks)
	if err != nil {
		return 0, err
	}
	vsBuf, err := common.Int32ToByteSlice(record.dateHeader.Vs)
	if err != nil {
		return 0, err
	}
	flagBuf, err := common.Int32ToByteSlice(record.dateHeader.Flag)
	if err != nil {
		return 0, err
	}
	recordBuf := common.AppendByteSlice(ksBuf, vsBuf, flagBuf, record.key, record.value)
	crc32 := common.CreateCrc32(recordBuf)
	crcBuf, err := common.UintToByteSlice(crc32)
	if err != nil {
		return 0, err
	}
	recordBuf = append(recordBuf, crcBuf...)
	db.mutex.Lock()																		// 互斥锁上锁
	recordLength, err := common.WriteFileWithByte(db.dataFile, db.size, recordBuf)
	if err != nil {
		return 0, err
	}
	db.size += recordLength
	defer db.mutex.Unlock()																// 解锁
	return recordLength, nil
}

func (db *AkitaDB)ReadRecord(offset int64, length int64) ([]byte, error) {
	recordBuf, err := common.ReadFileToByte(db.dataFile, offset, length)
	if err != nil {
		return nil, err
	}
	ksBuf := recordBuf[0:common.KsByteLength:common.KsByteLength]
	ks, err := common.ByteSliceToInt32(ksBuf)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	valueBuf     := recordBuf[(common.KvsByteLength + common.FlagByteLength + int64(ks) - 1):(length - common.CrcByteLength)]
	crcSrcBuf    := recordBuf[0:(length - common.CrcByteLength)]
	recordCrcBuf := recordBuf[(length - common.CrcByteLength - 1):length]
	checkCrc32, err := common.ByteSliceToUint(recordCrcBuf)
	if err != nil {
		return nil, err
	}
	crc32 := common.CreateCrc32(crcSrcBuf)
	if crc32 != checkCrc32 {
		return nil, common.ErrDataHasBeenModified
	}
	return valueBuf, nil
}

func (conn *Connection) Close() error {									// 关闭连接, 使 Connection 实现 io.Closer
	return nil
}



