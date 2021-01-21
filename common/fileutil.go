package common

import (
	"os"
)

// ReadFileToBytes read data from src.
func ReadFileToBytes(src *os.File, offset int64, length int64) ([]byte, error) {
	buff := make([]byte, length)
	if _, err := src.ReadAt(buff, offset); err != nil {
		return nil, err
	}
	return buff, nil
}

// WriteBufToFile write data to dst.
func WriteBufToFile(dst *os.File, offset int64, buff []byte) (int64, error) {
	length, err := dst.WriteAt(buff, offset)
	if err != nil {
		return 0, err
	}
	return int64(length), nil
}

// GetFileSize get file size.
func GetFileSize(src *os.File) (int64, error) {
	bufLen, err := src.Seek(0, 2)
	defer src.Seek(0, 0)
	return bufLen, err
}

// FileIsExit determine whether the file exists.
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
