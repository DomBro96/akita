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
	akMap  := SingletonCoreMap()
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

func (conn *Connection) Seek(key string) ([]byte, error) {				// 查找数据
	//TODO:
	// 1. 减少不必要的文件读写
	// 2. 使用 slice 特性
	// 3. 使用 goroutine
	akMap := SingletonCoreMap()
	offset, err := akMap.get(key)										    // 获取该记录的起始 offset
	if err != nil {
		return nil, err
	}
	crc32Chan := make(chan uint32, 2) 										// 通道中传递检验的 crc32 和取出该记录的crc32
	// TODO: 1. 使用带有缓冲的通道  2. 只对文件进行两次读取
	ksBuf, err := common.ReadFileToByte(common.DefaultDataFile, offset, common.KsByteLength)
	if err != nil {
		return nil, err
	}
	vsBuf, err  := common.ReadFileToByte(common.DefaultDataFile, offset + common.KsByteLength, common.VsByteLength)
	if err != nil {
		return nil, err
	}
	flagBuf, err := common.ReadFileToByte(common.DefaultDataFile, offset + common.KsByteLength + common.VsByteLength, common.FlagByteLength)
	if err != nil {
		return nil, err
	}
	ks, err := utils.ByteSliceToInt32(ksBuf)
	if err != nil {
		return nil, err
	}
	keyBuf, err := common.ReadFileToByte(common.DefaultDataFile, offset + common.KsByteLength + common.VsByteLength + common.FlagByteLength, int64(ks))
	if err != nil {
		return nil, err
	}
	vs, err := utils.ByteSliceToInt32(vsBuf)
	valueBuf, err := common.ReadFileToByte(common.DefaultDataFile, offset + common.KsByteLength + common.VsByteLength + common.FlagByteLength + int64(ks), int64(vs))
	if err != nil {
		return nil, err
	}
	crcBuf, err := common.ReadFileToByte(common.DefaultDataFile, offset + common.KsByteLength + common.VsByteLength + common.FlagByteLength + int64(ks) + int64(vs), common.CrcByteLength)
	getCrc, err := utils.ByteSliceToUint(crcBuf)
	crcSlice := utils.AppendByteSlice(ksBuf, vsBuf, flagBuf, keyBuf, valueBuf)
	crcCheck := utils.CreateCrc32(crcSlice)
	if crcCheck != getCrc {												// 如果 crc 检验不成功
		return nil, common.ErrDataHasBeenModified
	}
	return valueBuf, nil
}

func (conn *Connection) Delete(key string) (bool, error)  {				// 删除数据
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

func ReadRecord(filePath string )  {

}

func (conn *Connection) Close() error {									// 关闭连接, 使 Connection 实现 io.Closer
	return nil
}



