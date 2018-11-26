package utils

import (
	"bytes"
	"encoding/binary"
)

func ByteSliceToInt(bufByte []byte) (int32, error)  {
	buf := bytes.NewBuffer(bufByte)
	var i int32
	//同样使用大端法读取
	err := binary.Read(buf, binary.BigEndian, &i)
	return i, err
}

func IntToByteSlice(i int32) ([]byte, error) {
	s1 := make([]byte, 0)
	buf := bytes.NewBuffer(s1)
	// int64 to []byte, 使用大端法
	err := binary.Write(buf, binary.BigEndian, i)
	bufByte := buf.Bytes()
	return bufByte, err

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