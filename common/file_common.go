package common

import (
	"fmt"
	"os"
)

func ReadFileToByte(src *os.File, offset int64, length int64) ([]byte, error) {
	buff := make([]byte, length)
	_, err := src.ReadAt(buff, offset)
	if err != nil {
		fmt.Printf("read file error: %s\n", err)
		return nil, err
	}
	return buff, nil
}

func WriteBufToFile(dst *os.File, offset int64, buff []byte) (int64, error) {
	length, err := dst.WriteAt(buff, offset)
	if err != nil {
		return 0, err
	}
	return int64(length), nil
}

func GetFileSize(src *os.File) (int64, error) {
	bufLen, err := src.Seek(0, 2)
	defer src.Close()
	defer src.Seek(0, 0)
	return bufLen, err
}

func FileIsExit(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
