package common

import (
	"akita/ahttp"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
	"reflect"
	"time"
	"unsafe"
)

func ByteSliceToInt32(bufByte []byte) (int32, error) {
	buf := bytes.NewBuffer(bufByte)
	var i int32
	err := binary.Read(buf, binary.BigEndian, &i) // use big end way read bytes to int32
	return i, err
}

func Int32ToByteSlice(i int32) ([]byte, error) {
	s1 := make([]byte, 0)
	buf := bytes.NewBuffer(s1)
	err := binary.Write(buf, binary.BigEndian, i) // use big end way write int32 to int32
	bufByte := buf.Bytes()
	return bufByte, err
}

func UintToByteSlice(u uint32) ([]byte, error) {
	s1 := make([]byte, 0)
	buf := bytes.NewBuffer(s1)
	err := binary.Write(buf, binary.BigEndian, u) // use big end way write uint32 to int32
	bufByte := buf.Bytes()
	return bufByte, err
}

func ByteSliceToUint(bufByte []byte) (uint32, error) {
	buf := bytes.NewBuffer(bufByte)
	var u uint32
	err := binary.Read(buf, binary.BigEndian, &u) // use big end way read bytes to uint32
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

func AppendByteSlice(bs ...[]byte) []byte {
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

func GetExternalIp() (string, error) {
	hc := ahttp.NewHttpClient(2000 * time.Millisecond)
	data, err := hc.Get("http://myexternalip.com/raw")
	if err != nil {
		return "", err
	}
	str := ByteSliceToString(data)
	fmt.Println(str)
	return str, nil
}

func GetIntranetIp() (string, error) {
	adds, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	var intranetIp string
	for _, a := range adds {
		if ipNet, ok := a.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				intranetIp = ipNet.IP.String()
			}
		}
	}
	return intranetIp, nil
}

