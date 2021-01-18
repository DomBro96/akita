package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strconv"
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
	recordBuf, err := d.genRecordBuf(record, false)
	if err != nil {
		t.Errorf("record get buf error: %s.\n", err)
		return
	}
	t.Logf("test write record =====> record bytes len: %d. \n", len(recordBuf))

	go func(db *DB) {
		t.Logf("test write record from RecordQueue=====>  \n")
		db.WriteFromRecordQueue()
	}(d)

	err = d.WriteRecord(record)

	t.Logf("test write record err : %v.\n", err)
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

	err := d.WriteRecordNoCrc32(record)
	if err != nil {
		t.Errorf("write record no crc32 err: %s. \n", err)
		return
	}
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
		t.Logf("test update db table error: %s. \n", err)
		return
	}
}

func Test_GetDataByOffset(t *testing.T) {
	t.Log("test get data by offset.")
	d := OpenDB("/usr/local/akdata/akita.dat")

	t.Logf("test get data by offset =====> size: %d. \n", d.size)

	data, err := d.GetDataByOffset(0)
	if err != nil {
		t.Errorf("test get data by offset error: %s. \n", err)
		return
	}

	t.Logf("test get data by offset =====> data lenth :%d. \n", len(data))
}

func Test_WriteSyncData(t *testing.T) {
	t.Log("test write sync data.")

	d := OpenDB("/usr/local/akdata/akita.dat")
	key := "test2"
	value := []byte{5, 4, 3, 2, 1}
	keyBytes := []byte(key)
	ks, vs := len(keyBytes), len(value)

	t.Logf("test write sync data =====> key: %s,  value: %v, ks: %d, vs: %d. \n", key, value, ks, vs)

	record := &dataRecord{
		dateHeader: &dataHeader{
			Ks:   int32(ks),
			Vs:   int32(vs),
			Flag: 1,
		},
		key:   keyBytes,
		value: value,
	}
	recordBuf, err := d.genRecordBuf(record, false)
	if err != nil {
		t.Errorf("record get buf error: %s.\n", err)
		return
	}

	t.Logf("test write sync data =====> record bytes len: %d. \n", len(recordBuf))

	c32 := crc32.ChecksumIEEE(recordBuf)
	crcBytes, err := uintToByteSlice(c32)
	if err != nil {
		t.Errorf("record get crc bytes error: %s.\n", err)
		return
	}
	recordBuf = append(recordBuf, crcBytes...)

	key1 := "test3"
	value1 := []byte{9, 8, 7, 6, 5}
	keyBytes1 := []byte(key1)
	ks1, vs1 := len(keyBytes1), len(value1)

	t.Logf("test write sync data =====> key1: %s,  value1: %v, ks1: %d, vs1: %d. \n", key1, value1, ks1, vs1)

	record1 := &dataRecord{
		dateHeader: &dataHeader{
			Ks:   int32(ks1),
			Vs:   int32(vs1),
			Flag: 1,
		},
		key:   keyBytes1,
		value: value1,
	}
	recordBuf1, err := d.genRecordBuf(record1, false)
	if err != nil {
		t.Errorf("record get buf error: %s.\n", err)
		return
	}

	t.Logf("test write sync data =====> record bytes len: %d. \n", len(recordBuf1))

	c321 := crc32.ChecksumIEEE(recordBuf1)
	crcBytes1, err := uintToByteSlice(c321)
	if err != nil {
		t.Errorf("record get crc bytes error: %s.\n", err)
		return
	}
	recordBuf1 = append(recordBuf1, crcBytes1...)

	recordBuf = append(recordBuf, recordBuf1...)

	t.Logf("test write sync data =====> sync record bytes len: %d. \n", len(recordBuf))

	if err := d.WriteSyncData(recordBuf); err != nil {
		t.Errorf("write sync data error: %s.\n", err)
	}

}

func uintToByteSlice(u uint32) ([]byte, error) {
	s1 := make([]byte, 0)
	buf := bytes.NewBuffer(s1)
	err := binary.Write(buf, binary.BigEndian, u)
	bufByte := buf.Bytes()
	return bufByte, err
}

func BenchmarkWriteRecord(b *testing.B) {
	b.Log("bechmark write record.")
	d := OpenDB("/usr/local/akdata/akita.dat")
	go func(db *DB) {
		b.Logf("benchmark write record from RecordQueue...\n")
		db.WriteFromRecordQueue()
	}(d)
	keyPre := "benchmark"
	for i := 0; i < b.N; i++ {
		key := keyPre + strconv.Itoa(i)
		value := make([]byte, i+1)
		aktiaWriteRecord(d, key, value)
	}

}

func aktiaWriteRecord(db *DB, key string, value []byte) {
	keyBytes := []byte(key)
	ks, vs := len(keyBytes), len(value)

	record := &dataRecord{
		dateHeader: &dataHeader{
			Ks:   int32(ks),
			Vs:   int32(vs),
			Flag: 1,
		},
		key:   keyBytes,
		value: value,
	}
	err := db.WriteRecord(record)
	fmt.Printf("bechmark write record =====> err:%v. \n", err)
}
