package db

import (
	"akita/common"
	"akita/utils"
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
	//TODO:
	// 1. 数据记录插入文件
	// 2. 数据插入索引 map
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
	header := &DataHeader{Ks: int32(ks), Vs: int32(vs), Flag: 1}				//DbHeader
	ksBuf, err := utils.IntToByteSlice(header.Ks)
	vsBuf, err := utils.IntToByteSlice(header.Vs)
	flagBuf, err := utils.IntToByteSlice(header.Flag)
	recordBuf := utils.AppendByteSlice(ksBuf, vsBuf, flagBuf, keyBuf, valueBuf)
	crc32  := utils.CreateCrc32(recordBuf)
	header.Crc = crc32
	crcBuf, err := utils.UintToByteSlice(crc32)
	recordBuf = append(recordBuf, crcBuf...)
	//record := DataRecord{dateHeader: header, key: keyBuf, value: valueBuf}
	akMap  := SingletonCoreMap()
	offset, err := common.WriteFileWithByte(common.DefaultDataFile, akMap.CurOffset, recordBuf)
	if err != nil {
		return false, err
	}
	akMap.Map[key] = akMap.CurOffset
	akMap.CurOffset = offset
	return true, nil
}

func (conn *Connection) Lookup(key string) ([]byte, error) {					//查找数据
	return nil, nil
}

func (conn *Connection) Delete(key string) (bool, error)  {						//删除数据
	return false,  nil
}

func (conn *Connection) Close() error {											//关闭连接, 使 Connection 实现 io.Closer
	return nil
}

