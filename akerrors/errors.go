package akerrors

import "errors"

var (
	ErrKeySize             = errors.New("key size is too large to save. ")
	ErrDataHasBeenModified = errors.New("the data has been modified, not safe. ")
	ErrNoDataUpdate        = errors.New("no data update. ")
)
