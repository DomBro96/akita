package db

import (
	"sync"
	"unsafe"
)

type (
	recordIndex struct {
		offset int64 // record begin offset
		size   int64 // record size
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

// put insert record to index table.
func (it *indexTable) put(key string, newIndex *recordIndex) (oldIndex *recordIndex) {
	it.rwLock.Lock()
	defer it.rwLock.Unlock()
	oldIndex = it.table[key]
	it.table[key] = newIndex
	if oldIndex == nil {
		it.usage += len(key) + recordIndexSize
	}
	return
}

// find record from index table.
func (it *indexTable) get(key string) (ri *recordIndex) {
	it.rwLock.RLock()
	defer it.rwLock.RUnlock()
	ri = it.table[key]
	return
}

// delete record from index table.
func (it *indexTable) remove(key string) *recordIndex {
	it.rwLock.Lock()
	defer it.rwLock.Unlock()
	if index, exists := it.table[key]; exists {
		it.usage -= len(key) + recordIndexSize
		delete(it.table, key)
		return index
	}
	return nil
}
