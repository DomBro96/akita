package db

import (
	"sync"
)

type (
	keyExpire struct {
		key     string
		seconds int64 // key expires after seconds
	}
	// keyExpireHeap represents the small top heap of expired keys
	keyExpireHeap struct {
		sync.Mutex
		keyExpires []*keyExpire
		size       int
		cap        int
	}
)

func newKeyExpireHeap(c int) *keyExpireHeap {
	return &keyExpireHeap{
		keyExpires: make([]*keyExpire, c),
		size:       0,
		cap:        c,
	}
}

// 1. move tail node to the top
// 2. adjust small top heap
func (h *keyExpireHeap) pop() *keyExpire {
	h.Lock()
	defer h.Unlock()
	if h.size == 0 {
		h.keyExpires = make([]*keyExpire, h.cap)
		return nil
	}
	top := h.keyExpires[0]
	h.size--

	// dynamic expansion
	if h.size <= cap(h.keyExpires)/2 && cap(h.keyExpires) > h.cap {
		nks := make([]*keyExpire, h.size)
		for j := 0; j < h.size; j++ {
			nks[j] = h.keyExpires[j]
		}
		h.keyExpires = nks
	}

	tail := h.keyExpires[h.size]
	i := 0
	for i*2+1 < h.size {
		// child node index
		ci, rci := i*2+1, i*2+2
		if rci > h.size && h.keyExpires[rci].seconds < h.keyExpires[ci].seconds {
			ci = rci
		}
		if h.keyExpires[ci].seconds > tail.seconds {
			break
		}
		h.keyExpires[i] = h.keyExpires[ci]
		i = ci
	}
	h.keyExpires[i] = tail
	return top
}

func (h *keyExpireHeap) push(k *keyExpire) {
	h.Lock()
	defer h.Unlock()
	i := h.size
	// dynamic expansion
	if i == h.cap-1 {
		nks := make([]*keyExpire, 2*h.cap)
		for j := 0; j < h.cap; j++ {
			nks[j] = h.keyExpires[j]
		}
		h.keyExpires = nks
	}
	h.size++

	for i > 0 {
		// parent node index
		parentIdx := (i - 1) / 2
		if h.keyExpires[parentIdx].seconds <= k.seconds {
			break
		}
		// parent node dive
		h.keyExpires[i] = h.keyExpires[parentIdx]
		i = parentIdx
	}
	h.keyExpires[i] = k
}
