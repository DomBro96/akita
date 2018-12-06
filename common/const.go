package common

import "errors"

const (
	DefaultDataFile = "/tmp/akdb/test.dat"
	K               = 1024
	M               = 1024 * 1024
	KsByteLength	= 4
	VsByteLength    = 4
	FlagByteLength  = 4
	CrcByteLength   = 4
)

var (
	ErrFileSize = errors.New("file size is too large to save. ")
	ErrKeySize  = errors.New("key size is too large to save. ")
	ErrNoSuchRecord = errors.New("no such record in database. ")
	ErrDataHasBeenModified = errors.New("the data has been modified, not safe. ")
)