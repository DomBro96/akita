package utils

import (
	"fmt"
	"testing"
)

func TestByteSliceToInt(t *testing.T) {

}

func TestIntToByteSlice(t *testing.T) {
	var i int32
	i = 1024
	bufByte, err := IntToByteSlice(i)
	if err != nil {
		fmt.Println("int to  byte slice  err: ", err)
	}
	fmt.Println(bufByte, len(bufByte))

	i2, err := ByteSliceToInt(bufByte)
	if err != nil {
		fmt.Println(" byte slice to int err: ", err)
	}
	fmt.Println(i2)
}

func TestStringToByteSlice(t *testing.T) {
	s := "hello Wold ！"
	bufByte:= StringToByteSlice(s)
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
	bs0, err := IntToByteSlice(10)
	bs1, err := IntToByteSlice(15)
	bs2, err := IntToByteSlice(20)
	if err != nil {
		fmt.Printf("int to slice err %s", err)
	}
	bs3 := AppendByteSlice(bs0, bs1, bs2)
	fmt.Println(bs3)
}
