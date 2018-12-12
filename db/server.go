package db

import (
	"akita/common"
	"github.com/labstack/echo"
)

type Server struct {
	master string     // 主库 ip
	slaves []string   // 从库 ip
	db     *DB        // DB 属性
	echo   *echo.Echo // echo Server 连接
}

func (server *Server) Insert(key string, fileName string) (bool, error) { // 插入数据
	keyBuf := common.StringToByteSlice(key)
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
	if vs > 10*common.M {
		return false, common.ErrFileSize
	}
	dataRecord := &DataRecord{
		dateHeader: &DataHeader{
			Ks:   int32(ks),
			Vs:   int32(vs),
			Flag: common.WriteFlag,
		},
		key:   keyBuf,
		value: valueBuf,
	}
	db := server.db
	offset := db.size
	errorChan := make(chan error)
	lengthChan := make(chan int64)
	go func(record *DataRecord) {
		length, err := db.WriteRecord(record)
		errorChan <- err
		lengthChan <- length
	}(dataRecord)

	if err = <-errorChan; err != nil {
		return false, err
	}
	it := db.iTable
	ri := &recordIndex{offset: offset, size: int(<-lengthChan)}
	it.put(key, ri) // 设置 map 索引
	return true, nil
}

func (server *Server) Seek(key string) ([]byte, error) {
	db := server.db
	it := server.db.iTable
	ri := it.get(key) // 获取该记录的起始 offset
	if ri == nil {
		return nil, nil
	}
	valueChan := make(chan []byte)
	errChan := make(chan error)
	go func() {
		value, err := db.ReadRecord(ri.offset, int64(ri.size))
		valueChan <- value
		errChan <- err
	}()
	if err := <-errChan; err != nil {
		return nil, err
	}
	value := <-valueChan
	return value, nil
}

func (server *Server) Delete(key string) (bool, int64, error) { // 删除数据, 返回删除数据的 offset
	db := server.db
	it := db.iTable
	ri := it.remove(key)
	if ri == nil {
		return false, 0, nil
	}
	keyBuf := common.StringToByteSlice(key)
	ks := len(keyBuf)
	dataRecord := &DataRecord{
		dateHeader: &DataHeader{
			Ks:   int32(ks),
			Vs:   int32(0),
			Flag: common.DeleteFlag,
		},
		key:   keyBuf,
		value: nil,
	}
	errChan := make(chan error)
	go func(filePath string, from int64, record *DataRecord) {
		_, err := db.WriteRecord(record)
		errChan <- err
	}(common.DefaultDataFile, db.size, dataRecord)

	if err := <-errChan; err != nil {
		return false, 0, err
	}
	return true, ri.offset, nil
}
