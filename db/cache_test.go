package db

import (
	"fmt"
	"testing"
)

func Test_LRU(t *testing.T) {
	lru := newHashTableLRUList(50)
	t.Logf("lru node bucket len: %d .\n", len(lru.bucket))

	t.Logf("lru bucket is nil: %v . \n", lru.bucket[0] == nil)
	t.Logf("lru bucket is nil: %v . \n", lru.bucket[30] == nil)

	for i := 0; i <= 20; i++ {
		key := fmt.Sprintf("ak_cache_%d", i)
		data := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		n := &hashTableLRUNode{
			key:  key,
			data: data,
		}
		lru.insert(n)
	}

	t.Logf("lru node count: %d, usage: %d .\n", lru.count, lru.usage)

	lru.traversePrint()

	key := fmt.Sprintf("ak_cache_%d", 0)
	n := lru.search(key)
	t.Logf("lru node: %v .\n", *n)
	t.Logf("lru head: %v .\n", *lru.head)
	t.Logf("lru tail: %v .\n", *lru.tail)
	t.Logf("lru hn: %v .\n", lru.head.next.key)
	t.Logf("lru tn: %v .\n", lru.tail.pre.key)

	lru.traversePrint()

}
