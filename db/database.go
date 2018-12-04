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
	valueBuf, err := common.ReadFileToByte(fileName, 0)
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
	curOffset, err := dataRecord.WriteRecord(common.DefaultDataFile, offset)
	if err != nil {
		return false, nil
	}
	akMap.set(key)													 	// 设置 map 索引
	atomic.CompareAndSwapInt64(&akMap.CurOffset, offset, curOffset)		// 设置当前offset
	return true, nil
}

func (conn *Connection) Seek(key string) ([]byte, error) {				//查找数据
	//TODO:
	//1. 在索引中拿到offset
	//2. 如果存在该数据， 读取出 value -> []byte
	// 1) 判断文件是否被改动， 若改动返回异常
	// 2) 若未改动， 返回[]byte
	return nil, nil
}

func (conn *Connection) Delete(key string) (bool, error)  {				//删除数据
	return false,  nil
}

func (conn *Connection) Close() error {									//关闭连接, 使 Connection 实现 io.Closer
	return nil
}

