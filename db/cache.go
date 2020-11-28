package db

import (
	"akita/common"
	"fmt"
	"sync"
	"unsafe"
)

const (
	hashTableLRUNodeSize = int(unsafe.Sizeof(hashTableLRUNode{}))
	hashTableBucketCap   = 131
)

type (
	hashTableLRUNode struct {
		key   string
		data  []byte
		pre   *hashTableLRUNode
		next  *hashTableLRUNode
		hNext *hashTableLRUNode // point to the next node when there is a hash conflict
	}

	hashTableLRUCache struct {
		sync.RWMutex
		head   *hashTableLRUNode
		tail   *hashTableLRUNode
		bucket []*hashTableLRUNode
		usage  int
		limit  int
		count  int
	}
)

// newHashTableLRUCache a factory func to create a hashTableLRUList obj
func newHashTableLRUCache(limit int) *hashTableLRUCache {
	lru := &hashTableLRUCache{
		head: &hashTableLRUNode{
			key: "ak_cache_head",
		},
		tail: &hashTableLRUNode{
			key: "ak_cache_tail",
		},
		bucket: make([]*hashTableLRUNode, hashTableBucketCap),
		usage:  0,
		limit:  limit,
		count:  0,
	}
	lru.head.next = lru.tail
	lru.tail.pre = lru.head
	for i := range lru.bucket {
		lru.bucket[i] = &hashTableLRUNode{
			key: fmt.Sprintf("bucket_head_%d", i),
		}
	}

	return lru
}

func (l *hashTableLRUCache) seekBucket(key string) int {
	return common.HashCode(key) % len(l.bucket)
}

func (l *hashTableLRUCache) isEmpty() bool {
	return l.count == 0
}

func (l *hashTableLRUCache) isFull() bool {
	return l.count >= l.limit
}

func (l *hashTableLRUCache) search(key string) *hashTableLRUNode {
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
		prn := n.pre
		nxn := n.next
		prn.next = nxn
		nxn.pre = prn
		hn := l.head.next
		hn.pre = n
		n.next = hn
		l.head.next = n
		n.pre = l.head
	}
	return n
}

// TODO: use key, value instead of n params.
func (l *hashTableLRUCache) insert(n *hashTableLRUNode) {
	l.Lock()
	defer l.Unlock()

	bi := l.seekBucket(n.key)
	cn := l.bucket[bi]
	for cn.hNext != nil {
		if cn.hNext.key == n.key {
			l.usage -= hashTableLRUNodeSize + len(n.key) + len(cn.hNext.data)
			cn.hNext = cn.hNext.hNext
			prn := cn.pre
			nxn := n.next
			prn.next = nxn
			nxn.pre = prn
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
	hn.pre = n
	n.next = hn
	l.head.next = n
	n.pre = l.head
	l.usage += hashTableLRUNodeSize + len(n.data) + len(n.key)
	l.count++
}

func (l *hashTableLRUCache) remove(key string) {
	l.Lock()
	defer l.Unlock()
	bi := l.seekBucket(key)
	cn := l.bucket[bi]
	for cn.hNext != nil {
		if cn.hNext.key == key {
			dn := cn.hNext
			prn := dn.pre
			nxn := dn.next
			prn.next = nxn
			nxn.pre = prn
			cn.hNext = dn.hNext
			l.count--
			l.usage -= hashTableLRUNodeSize + len(dn.key) + len(dn.data)
			break
		}
		cn = cn.hNext
	}
}

func (l *hashTableLRUCache) traversePrint() {
	cn := l.head
	for cn != nil {
		fmt.Printf("key: %s->", cn.key)
		cn = cn.next
	}
	fmt.Println()
}

func (l *hashTableLRUCache) removeAll() {
	hn := l.head
	tn := l.tail
	l.Lock()
	defer l.Unlock()
	hn.next = tn
	tn.pre = hn
	l.bucket = make([]*hashTableLRUNode, hashTableBucketCap)
	for i := range l.bucket {
		l.bucket[i] = &hashTableLRUNode{
			key: fmt.Sprintf("bucket_head_%d", i),
		}
	}
	l.usage = 0
	l.count = 0
}
