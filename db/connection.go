package db

import (
	"akita/common"
	"akita/utils"
	"sync/atomic"
)

// 所有的数据库操作，都需要获取 Connection
type Connection struct {

}

func (conn *Connection) Insert(key string, fileName string) (bool, error) {		//插入数据
	keyBuf := utils.StringToByteSlice(key)
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
	akMap  := getSingletonAkitaMap()
	akMap.AkMutex.RLock()
	offset := akMap.CurOffset										    // 加读锁
	akMap.AkMutex.RUnlock()												// 解读锁
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
	akMap.AkMutex.Lock()
	akMap.set(key)													 	     // 设置 map 索引
	akMap.AkMutex.Unlock()
	atomic.CompareAndSwapInt64(&akMap.CurOffset, offset, <-offsetChan)		 // 设置当前 offset
	return true, nil
}

func (conn *Connection) Seek(key string) ([]byte, error) {					 // 查找数据
	akMap := getSingletonAkitaMap()
	offset, err := akMap.get(key)										 	// 获取该记录的起始 offset
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

func (conn *Connection) Delete(key string) (bool, []byte, error)  {				 				// 删除数据, 返回删除的 []byte
	/**
		TODO:
		1. 删除 akitaMap 中记录
		2. 在数据文件中新增一条记录， flag 记录为 2， value size 为 0
		3. 更新当前 offset
	 */
	akMap := getSingletonAkitaMap()
	ok, delOffset, err := akMap.del(key)
	if !ok {
		return false, nil, err
	}
	keyBuf := utils.StringToByteSlice(key)
	ks := len(keyBuf)
	vs := 0
	header := &DataHeader{Ks: int32(ks), Vs: int32(vs), Flag: 2}
	dataRecord := &DataRecord{dateHeader: header, key: keyBuf, value: nil}
	offset := akMap.CurOffset
	errChan   := make(chan error, 2)
	offsetChan := make(chan int64)
	valueChan  := make(chan []byte)
	// 读取数据
	go func() {
		value, err := ReadRecord(common.DefaultDataFile, delOffset)
		valueChan <- value
		errChan <- err
	}()

	if err = <- errChan; err != nil {
		return false, nil, err
	}
	value := <- valueChan

	go func(filePath string, from int64, record *DataRecord) {
		curOffset, err := WriteRecord(filePath, from, record)
		offsetChan <- curOffset
		errChan  <- err
	}(common.DefaultDataFile, offset, dataRecord)

	if err = <- errChan; err != nil {
		return false, nil, err
	}

	atomic.CompareAndSwapInt64(&akMap.CurOffset, offset, <-offsetChan)		 // 设置当前 offset
	return true, value, nil
}
