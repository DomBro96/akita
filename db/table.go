package db

import (
	"sync"
	"unsafe"
)

type (
	recordIndex struct {
		offset int64 // 记录的起始位置
		size   int   // 该条记录的长度
	}

	indexTable struct {
		table  map[string]*recordIndex
		rwLock sync.RWMutex // 读写锁
		usage  int          // 索引占内存大小
	}
)

const (
	recordIndexSize = int(unsafe.Sizeof(recordIndex{}))
)

func newIndexTable() *indexTable {
	return &indexTable{
		table: make(map[string]*recordIndex, 1024),
	}
}

func (it *indexTable) put(key string, newIndex *recordIndex) (oldIndex *recordIndex) { // 将记录放入索引表
	it.rwLock.Lock()
	oldIndex = it.table[key]
	it.table[key] = newIndex
	if oldIndex == nil { // 如果索引是新插入的， 更新索引表内存大小
		it.usage += len(key) + recordIndexSize
	}
	defer it.rwLock.Unlock()
	return
}

func (it *indexTable) get(key string) (ri *recordIndex) { // 在索引中查找
	it.rwLock.RLock()
	ri = it.table[key]
	defer it.rwLock.RUnlock()
	return
}

func (it *indexTable) remove(key string) (ri *recordIndex) { // 在索引中删除, 返回删除是否成功以及 value
	it.rwLock.Lock()
	if ri = it.table[key]; ri != nil {
		it.usage -= len(key) + recordIndexSize
		delete(it.table, key)
	}
	defer it.rwLock.Unlock()
	return
}
