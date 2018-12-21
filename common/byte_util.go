package common

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"reflect"
	"unsafe"
)

func ByteSliceToInt32(bufByte []byte) (int32, error) {
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

func UintToByteSlice(u uint32) ([]byte, error) {
	s1 := make([]byte, 0)
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

func StringToByteSlice(str string) (buf []byte) {
	if str == "" {
		return nil
	}
	strHeader := (*reflect.StringHeader)(unsafe.Pointer(&str))
	sliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&buf))
	sliceHeader.Len = strHeader.Len
	sliceHeader.Cap = strHeader.Len
	sliceHeader.Data = strHeader.Data
	return
}

func ByteSliceToString(buf []byte) (str string) {
	if buf == nil {
		return ""
	}
	sliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&buf))
	strHeader := (*reflect.StringHeader)(unsafe.Pointer(&str))
	strHeader.Len = sliceHeader.Len
	strHeader.Data = sliceHeader.Data
	return
}

func AppendByteSlice(bs ...[]byte) []byte { // 将若干切片追加到一起
	buf := make([]byte, 0)
	for _, b := range bs {
		buf = append(buf, b...)
	}
	return buf
}

func CreateCrc32(buf []byte) uint32 {
	crcValue := crc32.ChecksumIEEE(buf)
	return crcValue
}
