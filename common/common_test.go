package common

import (
	"fmt"
	"strings"
	"testing"
)

func TestByteSliceToInt(t *testing.T) {

}

func TestIntToByteSlice(t *testing.T) {
	var i int32
	i = 1024
	bufByte, err := Int32ToByteSlice(i)
	if err != nil {
		fmt.Println("int to  byte slice  err: ", err)
	}
	fmt.Println(bufByte, len(bufByte))

	i2, err := ByteSliceToInt32(bufByte)
	if err != nil {
		fmt.Println(" byte slice to int err: ", err)
	}
	fmt.Println(i2)
}

func TestStringToByteSlice(t *testing.T) {
	s := "hello Wold ！"
	bufByte := StringToByteSlice(s)
	fmt.Println(bufByte, len(bufByte))
	fmt.Println(len(bufByte))
	s1 := ByteSliceToString(bufByte)
	fmt.Println(s1)
}

func TestUintToByteSlice(t *testing.T) {
	var u uint32 = 255
	buf, err := UintToByteSlice(u)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(buf)

	u0, err := ByteSliceToUint(buf)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(u0)
}

func TestAppendByteSlice(t *testing.T) {
	bs0, err := Int32ToByteSlice(10)
	bs1, err := Int32ToByteSlice(15)
	bs2, err := Int32ToByteSlice(20)
	if err != nil {
		fmt.Printf("int to slice err %s", err)
	}
	bs3 := AppendByteSlice(bs0, bs1, bs2)
	fmt.Println(bs3)
}
func TestConfig_InitConfig(t *testing.T) {
	c := new(Config)
	err := c.InitConfig("/Users/dombro/go/src/akita/file_test/akita.ini")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(c.Read("server.host"))
	fmt.Println(c.Read("server.port"))
	fmt.Println()
}

func TestConfig_Read(t *testing.T) {
	s := "{115.21.32.31,112.23.45.67,12.56.79.46}"
	s = strings.TrimSpace(s)
	fmt.Println(s)
	s = strings.Replace(s, "{", "", 1)
	s = strings.Replace(s, "}", "", 1)
	fmt.Println(s)
}

func TestGetExternalIp(t *testing.T) {
	GetExternalIp()
}

func TestGetIntranetIp(t *testing.T) {
	intranet, err := GetIntranetIp()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(intranet)
}

func TestLogger(t *testing.T) {
	Info.Println("hello")
	Warning.Println("world")
}
