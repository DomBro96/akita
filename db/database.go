package db

import (
	"akita/common"
	"akita/utils"
	"sync/atomic"
)

type AkitaDb struct {
}


// 所有的数据库操作，都需要获取 Connection
type Connection struct {
}



func (db *AkitaDb) Reload() (bool, error) {							   			//数据重新载入
	return false, nil
}

func (conn *Connection) Insert(key string, fileName string) (bool, error) {		//插入数据
	keyBuf  := utils.StringToByteSlice(key)
	bufLen, err := common.GetFileSize(fileName)
	if err != nil {
		return false, err
	}
	valueBuf, err := common.ReadFileToByte(fileName, 0, bufLen)
	if err != nil {
		return false, err
	}
	ks := len(keyBuf)
	vs := len(valueBuf)
	if ks > common.K {
		return false, common.ErrKeySize
	}
	if vs > 10 * common.M {
		return false, common.ErrFileSize
	}
	header := &DataHeader{Ks: int32(ks), Vs: int32(vs), Flag: 1}
	dataRecord := &DataRecord{dateHeader: header, key: keyBuf, value: valueBuf}
	akMap  := SingletonAkitaMap()
	offset := akMap.CurOffset
	offsetChan := make(chan int64)
	errorChan  := make(chan error)

	go func(filePath string, from int64, record *DataRecord) {
		curOffset, err := WriteRecord(filePath, from, record)
		offsetChan <- curOffset
		errorChan  <- err
	}(common.DefaultDataFile, offset, dataRecord)

	if err = <-errorChan; err != nil {
		return false, err
	}

	akMap.set(key)													 	     // 设置 map 索引
	atomic.CompareAndSwapInt64(&akMap.CurOffset, offset, <-offsetChan)		 // 设置当前 offset
	return true, nil
}

func (conn *Connection) Seek(key string) ([]byte, error) {				 // 查找数据
	akMap := SingletonAkitaMap()
	offset, err := akMap.get(key)										 // 获取该记录的起始 offset
	if err != nil {
		return nil, err
	}
	valueChan := make(chan []byte)
	errChan := make(chan error)

	go func() {
		value, err := ReadRecord(common.DefaultDataFile, offset)
		valueChan <- value
		errChan <- err
	}()

	if err = <- errChan; err != nil {
		return nil, err
	}
	value := <- valueChan
	return value, nil
}

func (conn *Connection) Delete(key string) (bool, error)  {				 // 删除数据
	return false,  nil
}

// 向数据文件中写入一条记录
func WriteRecord (dataFile string, offset int64, record * DataRecord) (int64, error) {	// 将记录写入
	ksBuf, err := utils.Int32ToByteSlice(record.dateHeader.Ks)
	if err != nil {
		return 0, err
	}
	vsBuf, err := utils.Int32ToByteSlice(record.dateHeader.Vs)
	if err != nil {
		return 0, err
	}
	flagBuf, err := utils.Int32ToByteSlice(record.dateHeader.Flag)
	if err != nil {
		return 0, err
	}
	recordBuf := utils.AppendByteSlice(ksBuf, vsBuf, flagBuf, record.key, record.value)
	crc32 := utils.CreateCrc32(recordBuf)
	crcBuf, err := utils.UintToByteSlice(crc32)
	if err != nil {
		return 0, err
	}
	recordBuf = append(recordBuf, crcBuf...)
	curOffset, err := common.WriteFileWithByte(dataFile, offset, recordBuf)
	if err != nil {
		return 0, err
	}
	return curOffset, nil
}

func ReadRecord(filePath string, offset int64) ([]byte, error) {
	kvsBuf, err := common.ReadFileToByte(filePath, offset, common.KvsByteLength)
	if err != nil {
		return nil, err
	}
	ksBuf := kvsBuf[0:common.KsByteLength:common.KsByteLength]
	vsBuf := kvsBuf[common.KsByteLength:len(kvsBuf):common.VsByteLength]
	ks, err := utils.ByteSliceToInt32(ksBuf)
	if err != nil {
		return nil, err
	}
	vs, err := utils.ByteSliceToInt32(vsBuf)
	if err != nil {
		return nil, err
	}
	fkvLength := common.FlagByteLength + int64(ks) + int64(vs)
	recordWithoutKvsBuf, err := common.ReadFileToByte(filePath, offset + common.KvsByteLength, fkvLength + common.CrcByteLength)
	if err != nil {
		return nil, err
	}
	flagKeyValBuf := recordWithoutKvsBuf[0:fkvLength]
	valueBuf := recordWithoutKvsBuf[fkvLength + int64(ks) - 1:fkvLength]
	crc32Buf := recordWithoutKvsBuf[fkvLength:]
	recordWithoutCrc32Buf := utils.AppendByteSlice(kvsBuf, flagKeyValBuf)
	recordCrc32, err := utils.ByteSliceToUint(crc32Buf)
	if err != nil {
		return nil, err
	}
	checkCrc32 := utils.CreateCrc32(recordWithoutCrc32Buf)
	if err != nil {
		return nil, err
	}
	if recordCrc32 != checkCrc32 {
		return nil, common.ErrDataHasBeenModified
	}
	return valueBuf, nil
}

func (conn *Connection) Close() error {									// 关闭连接, 使 Connection 实现 io.Closer
	return nil
}



