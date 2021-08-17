package memtable

import (
	"math/rand"
	"sync/atomic"
	"time"
)

const (
	DefaultSkiplistMemNodeForwardLen  = 5
	DefaultSkiplistMemSkipProbability = 0.5
	DefaultSkiplistHeight             = 5
	DefaultSkiplistMaxLevel           = 1
	ExpireAtNeverExpire               = -1
	SkiplistMaxCasRotation            = 3
)

const (
	SkiplistCasStateDefault = iota
	SkiplistCasStateInsert
	SkiplistCasStateDelete
)

// SkiplistNode represent SkiplistMem‘s node.
type SkiplistNode struct {
	key      string
	value    []byte
	expireAt int64
	forwards []*SkiplistNode // forwards save the forward node pointer of level[n]
	level    int
}

// NewSkiplistNode create a new SkiplistMem Node
// parameter ‘fl’ is length of forwards
func NewSkiplistNode(k string, v []byte, e int64, l int, fl int) *SkiplistNode {
	if fl <= 0 {
		fl = DefaultSkiplistMemNodeForwardLen
	}
	if time.Unix(e, 0).Before(time.Now()) {
		e = ExpireAtNeverExpire
	}
	if l < 0 {
		l = 0
	}
	return &SkiplistNode{
		key:      k,
		value:    v,
		expireAt: e,
		forwards: make([]*SkiplistNode, fl),
		level:    l,
	}
}

func (s *SkiplistNode) Key() string {
	return s.key
}

func (s *SkiplistNode) Value() []byte {
	return s.value
}

func (s *SkiplistNode) ExpireAt() int64 {
	return s.expireAt
}

func (s *SkiplistNode) Less(n *SkiplistNode) bool {
	return s.key < n.Key()
}

// SkiplistMem represent Memtable which is a is a lock-free skip list data structure.
type SkiplistMem struct {
	head           *SkiplistNode
	height         int // height of skiplist
	highestLevel   int // the highest level of the skiplist node, highestLevel <= height
	levelP         float64
	size           int
	usage          int
	limit          int
	casState       int32
	maxCasRotation int
}

// NewSkiplistMem create a new SkiplistMem.
func NewSkiplistMem(h int, lp float64) *SkiplistMem {
	if lp <= 0 || lp >= 1 {
		lp = DefaultSkiplistMemSkipProbability
	}
	if h <= 0 {
		h = DefaultSkiplistHeight
	}
	return &SkiplistMem{
		head:           NewSkiplistNode("", nil, 0, 0, h),
		height:         h,
		highestLevel:   DefaultSkiplistMaxLevel,
		levelP:         lp,
		lastCASVisit:   NewSkiplistNode("", nil, 0, 0, h),
		casState:       SkiplistCasStateDefault,
		maxCasRotation: SkiplistMaxCasRotation,
	}
}

func (s *SkiplistMem) Insert(k string, v []byte, e int64) error {
	level := s.RandomLevel()
	n := NewSkiplistNode(k, v, e, level, s.height)
	return s.insertNode(n)
}

func (s *SkiplistMem) insertNode(n *SkiplistNode) error {
	if n == nil {
		return nil
	}

	rotation := 0
	for !atomic.CompareAndSwapInt32(&s.casState, SkiplistCasStateDefault, SkiplistCasStateInsert) {
		if rotation > s.maxCasRotation {
			s.casState = SkiplistCasStateDefault
		}
		rotation++
	}
	defer atomic.CompareAndSwapInt32(&s.casState, SkiplistCasStateInsert, SkiplistCasStateDefault)

	update := make([]*SkiplistNode, n.level)
	for i := 0; i < n.level; i++ {
		update[i] = s.head
	}
	curN := s.head
	for i := n.level - 1; i >= 0; i-- {
		for curN.forwards[i] != nil && curN.forwards[i].Less(n) {
			curN = curN.forwards[i]
		}
		update[i] = curN
	}
	for i := 0; i < n.level; i++ {
		// replace the same key node, O(1)
		fwN := update[i].forwards[i]
		for fwN != nil && fwN.key == n.key {
			fwN = fwN.forwards[i]
		}
		n.forwards[i] = fwN
		update[i].forwards[i] = n
	}
	if n.level > s.highestLevel {
		s.highestLevel = n.level
	}

	s.size++
	return nil
}

func (s *SkiplistMem) Delete(k string) error {

	rotation := 0
	for !atomic.CompareAndSwapInt32(&s.casState, SkiplistCasStateDefault, SkiplistCasStateDelete) {
		if rotation > s.maxCasRotation {
			s.casState = SkiplistCasStateDefault
		}
		rotation++
	}
	atomic.CompareAndSwapInt32(&s.casState, SkiplistCasStateDelete, SkiplistCasStateDefault)

	update := make([]*SkiplistNode, s.highestLevel)
	curN := s.head
	for i := s.highestLevel - 1; i >= 0; i-- {
		for curN.forwards[i] != nil && curN.forwards[i].key < k {
			curN = curN.forwards[i]
		}
		update[i] = curN
	}

	if curN.forwards[0] == nil || curN.forwards[0].key != k {
		return nil
	}

	for i := s.highestLevel - 1; i >= 0; i-- {
		if update[i].forwards[i] != nil && update[i].forwards[i].key == k {
			update[i].forwards[i] = update[i].forwards[i].forwards[i]
		}
	}
	s.size--
	return nil
}

func (s *SkiplistMem) Get(k string) *SkiplistNode {
	curN := s.head
	for i := s.highestLevel - 1; i >= 0; i-- {
		for curN.forwards[i] != nil && curN.forwards[i].key < k {
			curN = curN.forwards[i]
		}
	}
	if curN.forwards[0] != nil && curN.forwards[0].key == k {
		return curN.forwards[0]
	}
	return nil
}

func (s *SkiplistMem) Display() []*SkiplistNode {
	ns := make([]*SkiplistNode, 0, s.size)
	curN := s.head
	for curN != nil && curN.forwards[0] != nil {
		ns = append(ns, curN.forwards[0])
		curN = curN.forwards[0]
	}
	return ns
}

func (s *SkiplistMem) Full() bool {
	return s.usage >= s.limit
}

func (s *SkiplistMem) Size() int {
	return s.size
}

func (s *SkiplistMem) Usage() int {
	return s.usage
}

func (s *SkiplistMem) Limit() int {
	return s.limit
}

func (s *SkiplistMem) Flush() error {
	return nil
}

func (s *SkiplistMem) RandomLevel() int {
	level := 1
	n := int(1 / s.levelP)
	for i := 0; rand.Intn(n) == 1 && i < s.height-1; i++ {
		level++
	}
	return level
}
