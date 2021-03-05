package common

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"net"
	"reflect"
	"unsafe"
)

// ByteSliceToInt32 use big end way read bytes to int32
func ByteSliceToInt32(bufByte []byte) (int32, error) {
	buf := bytes.NewBuffer(bufByte)
	var i int32
	err := binary.Read(buf, binary.BigEndian, &i)
	return i, err
}

// ByteSliceToInt64 use big end way read bytes to int32
func ByteSliceToInt64(bufByte []byte) (int64, error) {
	buf := bytes.NewBuffer(bufByte)
	var i int64
	err := binary.Read(buf, binary.BigEndian, &i)
	return i, err
}

// Int32ToByteSlice use big end way write int32 to bytes
func Int32ToByteSlice(i int32) ([]byte, error) {
	s1 := make([]byte, 0)
	buf := bytes.NewBuffer(s1)
	err := binary.Write(buf, binary.BigEndian, i)
	bufByte := buf.Bytes()
	return bufByte, err
}

// Int64ByteSlice use big end way write int64 to bytes
func Int64ByteSlice(i int64) ([]byte, error) {
	s1 := make([]byte, 0)
	buf := bytes.NewBuffer(s1)
	err := binary.Write(buf, binary.BigEndian, i)
	bufByte := buf.Bytes()
	return bufByte, err
}

// UintToByteSlice use big end way write uint32 to bytes
func UintToByteSlice(u uint32) ([]byte, error) {
	s1 := make([]byte, 0)
	buf := bytes.NewBuffer(s1)
	err := binary.Write(buf, binary.BigEndian, u)
	bufByte := buf.Bytes()
	return bufByte, err
}

// ByteSliceToUint use big end way read bytes to uint32
func ByteSliceToUint(bufByte []byte) (uint32, error) {
	buf := bytes.NewBuffer(bufByte)
	var u uint32
	err := binary.Read(buf, binary.BigEndian, &u)
	return u, err
}

// StringToByteSlice using reflect pkg to convert string to byte slice
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

// ByteSliceToString using reflect pkg to convert byte slice to string
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

// AppendByteSlice append all byte slice to a new byte slice
func AppendByteSlice(bs ...[]byte) []byte {
	buf := make([]byte, 0)
	for _, b := range bs {
		buf = append(buf, b...)
	}
	return buf
}

// CreateCrc32 call crc32.ChecksumIEEE
func CreateCrc32(buf []byte) uint32 {
	return crc32.ChecksumIEEE(buf)
}

// GetIntranetIP get current intranet ip address
func GetIntranetIP() (string, error) {
	adds, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	var intranetIP string
	for _, a := range adds {
		if ipNet, ok := a.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				intranetIP = ipNet.IP.String()
			}
		}
	}
	return intranetIP, nil
}

// HashCode similar to the hashcode method in java
func HashCode(s string) int {
	h := 0
	if len(s) > 0 {
		for i := 0; i < len(s); i++ {
			h = 31*h + int(s[i])
		}
	}
	return h
}
