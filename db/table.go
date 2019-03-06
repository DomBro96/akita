package db

import (
	"sync"
	"unsafe"
)

type (
	recordIndex struct {
		offset int64 // record begin offset
		size   int   // record size
	}

	indexTable struct {
		table  map[string]*recordIndex
		rwLock sync.RWMutex
		usage  int // memory size of database index table
	}
)

const (
	// record index memory size
	recordIndexSize = int(unsafe.Sizeof(recordIndex{}))
)

func newIndexTable() *indexTable {
	return &indexTable{
		table: make(map[string]*recordIndex, 1024),
	}
}

func (it *indexTable) put(key string, newIndex *recordIndex) (oldIndex *recordIndex) { // insert record to index table
	it.rwLock.Lock()
	defer it.rwLock.Unlock()
	oldIndex = it.table[key]
	it.table[key] = newIndex
	if oldIndex == nil {
		it.usage += len(key) + recordIndexSize
	}
	return
}

func (it *indexTable) get(key string) (ri *recordIndex) { // find record from index table
	it.rwLock.RLock()
	defer it.rwLock.RUnlock()
	ri = it.table[key]
	return
}

func (it *indexTable) remove(key string) (ri *recordIndex) { // delete record from index table
	it.rwLock.Lock()
	defer it.rwLock.Unlock()
	if ri = it.table[key]; ri != nil {
		it.usage -= len(key) + recordIndexSize
		delete(it.table, key)
	}
	return
}
