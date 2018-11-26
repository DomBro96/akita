package common

import (
	"fmt"
	"os"
)

// read the file to a byte slice
func ReadFileToByte(filePath string, offset int64) ([]byte, error) {
	source, err := os.OpenFile(filePath, os.O_RDONLY, 0664)
	if err != nil{
		fmt.Printf("read file error: %s\n", err)
		return nil, err
	}
	defer source.Close()
	bufLen, err := GetFileSize(source)
	if err != nil {
		fmt.Printf("get file size error: %s\n", err)
		return nil, err
	}
	buff := make([]byte, bufLen)
	_, err = source.ReadAt(buff, offset)
	if err != nil {
		fmt.Printf("read file error: %s\n", err)
		return nil, err
	}

	return buff, nil
}


// write a byte slice to the file, return offset and error
func WriteFileWithByte(filePath string, offset int64, buff []byte) (int, error) {
	target, err :=  os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY|os.O_RDWR, 0664)
	if err != nil {
		fmt.Printf("open file error: %s\n", err)
		return 0, err
	}
	nOffset, err := target.WriteAt(buff, offset)
	if err != nil {
		fmt.Printf("write file error； %s\n", err)
		return 0, err
	}
	return nOffset, nil
}



func GetFileSize(file *os.File) (int64, error) {
	 bufLen, err := file.Seek(0, 2)
	 defer file.Seek(0, 0)
 	 return bufLen, err
}



