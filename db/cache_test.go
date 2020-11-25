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

	t.Logf("============== serach =============== \n")

	for i := 20; i >= 0; i-- {
		key := fmt.Sprintf("ak_cache_%d", i)
		n := lru.search(key)
		t.Logf("lru node: %v .\n", *n)
	}

	lru.traversePrint()

	t.Logf("============== remove =============== \n")

	lru.remove(fmt.Sprintf("ak_cache_%d", 0))
	lru.remove(fmt.Sprintf("ak_cache_%d", 1))
	lru.remove(fmt.Sprintf("ak_cache_%d", 25))
	lru.remove(fmt.Sprintf("ak_cache_%d", 7))

	t.Logf("lru node count: %d, usage: %d .\n", lru.count, lru.usage)
	lru.traversePrint()

	t.Logf("============== remove all =============== \n")
	for i := 20; i >= 0; i-- {
		key := fmt.Sprintf("ak_cache_%d", i)
		lru.remove(key)
	}
	lru.traversePrint()

}
