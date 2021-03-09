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
	if h.size <= len(h.keyExpires)/2 && h.size > h.cap {
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
	if h.size == len(h.keyExpires) {
		nks := make([]*keyExpire, 2*h.size)
		for j := 0; j < len(h.keyExpires); j++ {
			nks[j] = h.keyExpires[j]
		}
		h.keyExpires = nks
	}

	for i > 0 {
		// parent node index
		pi := (i - 1) / 2
		if h.keyExpires[pi].seconds <= k.seconds {
			break
		}
		// parent node dive
		h.keyExpires[i] = h.keyExpires[pi]
		i = pi
	}
	h.keyExpires[i] = k
	h.size++
}
