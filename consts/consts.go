package consts

const (
	K = 1 << 10
	M = 1 << 20
)

const (
	FlagWrite          = 1
	FlagDelete         = 2
	FlagExpire         = 3
	LengthKs           = 4
	LengthVs           = 4
	LengthFlag         = 4
	LengthExpireAt     = 8
	LengthCrc32        = 4
	LengthKVs          = LengthKs + LengthVs
	LengthRecordHeader = LengthKs + LengthVs + LengthFlag + LengthExpireAt
)
