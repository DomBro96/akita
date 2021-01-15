package common

// TODO move to other dir.
const (
	K                      = 1 << 10
	M                      = 1 << 20
	WriteFlag              = 1
	DeleteFlag             = 2
	KsByteLength           = 4
	VsByteLength           = 4
	FlagByteLength         = 4
	CrcByteLength          = 4
	KvsByteLength          = KsByteLength + VsByteLength
	RecordHeaderByteLength = KsByteLength + VsByteLength + FlagByteLength
)
