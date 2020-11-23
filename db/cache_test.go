package db

import (
	"fmt"
	"testing"
)

func Test_LRU(t *testing.T) {
	lru := newHashTableLRUList(50)
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

}
