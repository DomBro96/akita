package common

import (
	"errors"
)

const (
	K              = 1024
	M              = 1024 * 1024
	WriteFlag      = 1
	DeleteFlag     = 2
	KsByteLength   = 4
	VsByteLength   = 4
	FlagByteLength = 4
	CrcByteLength  = 4
	KvsByteLength  = KsByteLength + VsByteLength
	RecordHeaderByteLength = KsByteLength + VsByteLength + FlagByteLength
)

var (
	// TODO need move to other file
	ErrKeySize             = errors.New("key size is too large to save. ")
	ErrDataHasBeenModified = errors.New("the data has been modified, not safe. ")
	ErrNoDataUpdate        = errors.New("no data update. ")
)
