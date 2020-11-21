package db

import (
	"sync"
	"unsafe"
)

const (
	hashTableLRUNodeSize = int(unsafe.Sizeof(hashTableLRUNode{}))
	hashTableBucketCap   = 31 // TODO: need to be optimized later
)

type (
	hashTableLRUNode struct {
		key   string
		data  []byte
		pre   *hashTableLRUNode
		next  *hashTableLRUNode
		hNext *hashTableLRUNode // point to the next node when there is a hash conflict
	}

	hashTableLRUList struct {
		head   *hashTableLRUNode
		tail   *hashTableLRUNode
		bucket []*hashTableLRUNode
		usage  int
		sync.RWMutex
	}
)

// NewHashTableLRUList a factory func to create a hashTableLRUList obj
func newHashTableLRUList() *hashTableLRUList {
	return &hashTableLRUList{
		head: &hashTableLRUNode{
			key: "ak_cache_head",
		},
		tail: &hashTableLRUNode{
			key: "ak_cache_tail",
		},
		bucket: make([]*hashTableLRUNode, hashTableBucketCap),
		usage:  0,
	}
}

func (l *hashTableLRUList) hash(key string) int {
	return 0
}

func (l *hashTableLRUList) seekBucket(key string) int {
	return 0
}

func (l *hashTableLRUList) search(key string) *hashTableLRUNode {
	return nil
}

func (l *hashTableLRUList) insert(key string) *hashTableLRUNode {
	return nil
}

func (l *hashTableLRUList) delete(key string) *hashTableLRUNode {
	return nil
}
