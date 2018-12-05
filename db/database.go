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
	curOffset, err := dataRecord.WriteRecord(common.DefaultDataFile, offset)
	if err != nil {
		return false, nil
	}
	akMap.set(key)													 	// 设置 map 索引
	atomic.CompareAndSwapInt64(&akMap.CurOffset, offset, curOffset)		// 设置当前 offset
	return true, nil
}

func (conn *Connection) Seek(key string) ([]byte, error) {				// 查找数据
	//TODO:
	//1. 在索引中拿到offset
	//2. 如果存在该数据， 读取出 value -> []byte
	// 1) 判断文件是否被改动， 若改动返回异常
	// 2) 若未改动， 返回[]byte
	akMap := SingletonCoreMap()
	offset, err := akMap.get(key)										// 获取该记录的起始 offset
	if err != nil {
		return nil, err
	}
	ksBuf, err := common.ReadFileToByte(common.DefaultDataFile, offset, 4)
	if err != nil {
		return nil, err
	}
	vsBuf, err  := common.ReadFileToByte(common.DefaultDataFile, offset + 4, 4)
	if err != nil {
		return nil, err
	}
	flagBuf, err := common.ReadFileToByte(common.DefaultDataFile, offset + 8, 4)
	if err != nil {
		return nil, err
	}
	ks, err := utils.ByteSliceToInt64(ksBuf)
	keyBuf, err  := common.ReadFileToByte(common.DefaultDataFile, offset + 12, ks)
	if err != nil {
		return nil, err
	}
	vs, err := utils.ByteSliceToInt64(vsBuf)
	valueBuf, err := common.ReadFileToByte(common.DefaultDataFile, offset + 12 + ks, vs)
	if err != nil {
		return nil, err
	}
	crcSlice := utils.AppendByteSlice(ksBuf, vsBuf, flagBuf, keyBuf, valueBuf)
	crcCheck := utils.CreateCrc32(crcSlice)
	crcBuf, err := common.ReadFileToByte(common.DefaultDataFile, offset + 12 + ks + vs, 4)
	getCrc, err := utils.ByteSliceToUint(crcBuf)
	if crcCheck != getCrc {								// 如果 crc 检验不成功
		return nil, common.ErrDataHasBeenModified
	}
	return nil, nil
}

func (conn *Connection) Delete(key string) (bool, error)  {				// 删除数据
	return false,  nil
}

func (conn *Connection) Close() error {									// 关闭连接, 使 Connection 实现 io.Closer
	return nil
}

