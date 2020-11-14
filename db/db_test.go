package db

import (
	"hash/crc32"
	"testing"
)

func Test_OpenDB(t *testing.T) {
	t.Log("test op db.")
	d := OpenDB("/usr/local/akdata/akita.dat")
	t.Logf("db: %v.", d)
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

func Test_WriteRecordNoCrc32(t *testing.T) {
	t.Log("test write record no crc32.")
	d := OpenDB("/usr/local/akdata/akita.dat")
	key := "test1"
	keyBytes := []byte(key)
	ks := len(keyBytes)

	t.Logf("test write record no crc32=====> key: %s,  value: %v, ks: %d, vs: %d. \n", key, nil, ks, 0)

	record := &dataRecord{
		dateHeader: &dataHeader{
			Ks:   int32(ks),
			Vs:   int32(0),
			Flag: 2,
		},
		key:   keyBytes,
		value: nil,
	}

	size, err := d.WriteRecordNoCrc32(record)
	if err != nil {
		t.Errorf("write record no crc32 err: %s. \n", err)
		return
	}

	t.Logf("test write record no crc32 =====> size: %d.\n", size)

}

func Test_ReadRecord(t *testing.T) {
	t.Log("test read record.")
	d := OpenDB("/usr/local/akdata/akita.dat")
	recordBytes, err := d.ReadRecord(0, 26)
	if err != nil {
		t.Errorf("get record bytes error: %s.\n", err)
		return
	}

	t.Logf("test read record =====> get record bytes: %v. \n", recordBytes)
}

func Test_Reload(t *testing.T) {
	t.Log("test reload db index.")
	d := OpenDB("/usr/local/akdata/akita.dat")
	t.Logf("test reload db size =====> size: %d.\n", d.size)
	err := d.Reload()
	if err != nil {
		t.Errorf("reload db index error: %s.\n", err)
		return
	}
	table := d.iTable.table
	for k, v := range table {
		t.Logf("test reload db index =====> key: %s, value: %v. \n", k, *v)
	}
}

func Test_UpdateTable(t *testing.T) {
	t.Log("test update db index table.")
	d := OpenDB("/usr/local/akdata/akita.dat")
	t.Logf("test update index db table =====> size: %d.\n", d.size)

	err := d.UpdateTable(0, d.size)
	if err != nil {
		t.Logf("")
	}
}
