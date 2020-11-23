package db

import (
	"akita/common"
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
		sync.RWMutex
		head   *hashTableLRUNode
		tail   *hashTableLRUNode
		bucket []*hashTableLRUNode
		usage  int
		limit  int
		count  int
	}
)

// newHashTableLRUList a factory func to create a hashTableLRUList obj
func newHashTableLRUList(l int) *hashTableLRUList {
	lru := &hashTableLRUList{
		head: &hashTableLRUNode{
			key: "ak_cache_head",
		},
		tail: &hashTableLRUNode{
			key: "ak_cache_tail",
		},
		bucket: make([]*hashTableLRUNode, hashTableBucketCap),
		usage:  0,
		limit:  l,
		count:  0,
	}
	lru.head.next = lru.tail
	lru.tail.pre = lru.head
	return lru
}

func (l *hashTableLRUList) seekBucket(key string) int {
	return common.HashCode(key) % len(l.bucket)
}

func (l *hashTableLRUList) isEmpty() bool {
	return l.count == 0
}

func (l *hashTableLRUList) isFull() bool {
	return l.count >= l.limit
}

func (l *hashTableLRUList) search(key string) *hashTableLRUNode {
	bi := l.seekBucket(key)
	l.Lock()
	defer l.Unlock()
	var n *hashTableLRUNode
	cn := l.bucket[bi]
	for cn.hNext != nil {
		if cn.hNext.key == key {
			n = cn.hNext
			break
		}
		cn = cn.hNext
	}
	if n != nil {
		hn := l.head.next
		hn.pre = n
		n.next = hn
		l.head.next = n
		n.pre = l.head
	}
	return n
}

func (l *hashTableLRUList) insert(n *hashTableLRUNode) {
	l.Lock()
	defer l.Unlock()

	bi := l.seekBucket(n.key)
	cn := l.bucket[bi]
	for cn.hNext != nil {
		if cn.hNext.key == n.key {
			l.usage -= hashTableLRUNodeSize + len(n.key) + len(cn.hNext.data)
			cn.hNext = cn.hNext.hNext
		}
		cn = cn.hNext
	}
	cn.hNext = n

	if l.isFull() {
		tn := l.tail.pre
		pn := tn.pre
		pn.next = l.tail
		l.tail.pre = pn
		l.usage -= hashTableLRUNodeSize + len(pn.data) + len(pn.key)
	}

	hn := l.head.next
	l.head.next = n
	n.pre = l.head
	n.next = hn
	hn.pre = n
	l.usage += hashTableLRUNodeSize + len(n.data) + len(n.key)
	l.count++
}

func (l *hashTableLRUList) remove(key string) {
	l.Lock()
	defer l.Unlock()
	l.rm(key)
}

func (l *hashTableLRUList) rm(key string) {
	bi := l.seekBucket(key)
	cn := l.bucket[bi]
	for cn.hNext != nil {
		if cn.hNext.key == key {
			dn := cn.hNext
			pn := dn.pre
			nn := dn.next
			pn.next = nn
			nn.pre = pn
			cn.hNext = dn.hNext
			l.count--
			l.usage -= hashTableLRUNodeSize + len(dn.key) + len(dn.data)
		}
		cn = cn.hNext
	}
}
