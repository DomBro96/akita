package db

import (
	"hash/crc32"
	"testing"
)

func Test_OpenDB(t *testing.T) {
	t.Log("testing...")
	d := OpenDB("/usr/local/akdata/akita.dat")
	t.Logf("db: %v.", d)
	t.Log("testing end.")
}

func Test_WriteRecord(t *testing.T) {
	t.Log("test write record.")
	d := OpenDB("/usr/local/akdata/akita.dat")
	key := "test1"
	value := []byte{1, 2, 3, 4, 5}
	keyBytes := []byte(key)
	ks, vs := len(keyBytes), len(value)

	t.Logf("test write record =====> key: %s,  value: %v, ks: %d, vs: %d. \n", key, value, ks, vs)

	record := &dataRecord{
		dateHeader: &dataHeader{
			Ks:   int32(ks),
			Vs:   int32(vs),
			Flag: 1,
		},
		key:   keyBytes,
		value: value,
	}
	recordBuf, err := record.getRecordBuf()
	if err != nil {
		t.Errorf("record get buf error: %s.\n", err)
		return
	}

	t.Logf("test write record =====> record bytes len: %d. \n", len(recordBuf))

	crc32 := crc32.ChecksumIEEE(recordBuf)

	t.Logf("test write record =====> record bytes crc32: %d. \n", crc32)

	offset, size, err := d.WriteRecord(record)
	t.Logf("test write record =====> offset: %d, size: %d,  err:%s. \n", offset, size, err)

}
