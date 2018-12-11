package common

import (
	"bytes"
	"encoding/binary"
)

func ByteSliceToInt32(bufByte []byte) (int32, error)  {
	buf := bytes.NewBuffer(bufByte)
	var i int32
	//同样使用大端法读取
	err := binary.Read(buf, binary.BigEndian, &i)
	return i, err
}

func Int32ToByteSlice(i int32) ([]byte, error) {
	s1 := make([]byte, 0)
	buf := bytes.NewBuffer(s1)
	// int64 to []byte, 使用大端法
	err := binary.Write(buf, binary.BigEndian, i)
	bufByte := buf.Bytes()
	return bufByte, err
}

func UintToByteSlice(u uint32) ([]byte, error)  {
	s1  := make([]byte, 0)
	buf := bytes.NewBuffer(s1)
	err := binary.Write(buf, binary.BigEndian, u)
	bufByte := buf.Bytes()
	return bufByte, err
}

func ByteSliceToUint(bufByte []byte) (uint32, error) {
	buf := bytes.NewBuffer(bufByte)
	var u uint32
	//同样使用大端法读取
	err := binary.Read(buf, binary.BigEndian, &u)
	return u, err
}

func StringToByteSlice(key string) []byte   {
	 var data []byte
	 data = []byte(key)
	 return data
}

func ByteSliceToString(data []byte) string  {
	 var key string
	 key = string(data[:])
	 return key
}

func AppendByteSlice(bs ...[]byte) []byte {		// 将若干切片追加到一起
	buf := make([]byte, 0)
	for _, b := range bs {
		buf = append(buf, b...)
	}
	return buf
}