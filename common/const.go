package common

import "errors"

const (
	DefaultDataFile = "/tmp/akdb/test.dat"
	K               = 1024
	M               = 1024 * 1024
)

var (
	ErrFileSize = errors.New("file size is too large to save. ")
	ErrKeySize  = errors.New("key size is too large to save. ")
)