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
	fmt.Println(bufByte)

	i2, err := ByteSliceToInt(bufByte)
	if err != nil {
		fmt.Println(" byte slice to int err: ", err)
	}
	fmt.Println(i2)
}

func TestStringToByteSlice(t *testing.T) {
	s := "hello Wold ！"
	bufByte:= StringToByteSlice(s)
	fmt.Println(bufByte)
	fmt.Println(len(bufByte))
	s1 := ByteSliceToString(bufByte)
	fmt.Println(s1)
}