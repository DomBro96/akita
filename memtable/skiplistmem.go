package memtable

import "crypto/rand"

var _ MemtableNode = new(SkiplistMemNode)
var _ Memtable = new(SkiplistMem)

const (
	DefaultSkiplistMemNodeForwardLen  = 10
	DefaultSkiplistMemSkipProbability = 0.5
)

// SkiplistMemNode implements MemtableNode which as SkiplistMem‘s node.
type SkiplistMemNode struct {
	key      string
	value    []byte
	expireAt int64
	// forwards save the node pointer of level[n]
	forwards []*SkiplistMemNode
	level    int
}

// NewSkiplistNode create a new SkiplistMem Node
// parameter ‘fl’ is length of forwards
func NewSkiplistNode(k string, v []byte, e int64, l int, fl int) *SkiplistMemNode {
	if fl <= 0 {
		fl = DefaultSkiplistMemNodeForwardLen
	}
	return &SkiplistMemNode{
		key:      k,
		value:    v,
		expireAt: e,
		forwards: make([]*SkiplistMemNode, fl),
		level:    l,
	}
}

func (s *SkiplistMemNode) Key() string {
	return s.key
}

func (s *SkiplistMemNode) Value() []byte {
	return s.value
}

func (s *SkiplistMemNode) ExpireAt() int64 {
	return s.expireAt
}

func (s *SkiplistMemNode) Less(n MemtableNode) bool {
	return s.key < n.Key()
}

// SkiplistMem implements Memtable which is a is a lock-free skip list data structure.
type SkiplistMem struct {
	head     *SkiplistMemNode
	height   int
	maxLevel int
	levelP   float64
	size     int
	usage    int
	limit    int
}

// NewSkiplistMem create a new SkiplistMem.
func NewSkiplistMem(h int, lp float64) *SkiplistMem {
	if lp <= 0 || lp >= 1 {
		lp = DefaultSkiplistMemSkipProbability
	}
	return &SkiplistMem{
		head:     NewSkiplistNode("", nil, 0, 0, h),
		height:   h,
		maxLevel: 1,
		levelP:   lp,
	}
}

func (s *SkiplistMem) Insert(n MemtableNode) error {
	if n == nil {
		return nil
	}
	level := s.RandomLevel()
	update := make([]*SkiplistMemNode, level)
	for i := 0; i < level; i++ {
		update[i] = s.head
	}

	pN := s.head
	for i := level - 1; i >= 0; i++ {
		for pN.forwards[i] != nil && pN.forwards[i].Less(n) {
			pN = pN.forwards[i]
		}
		update[i] = pN
	}
	return nil
}

func (s *SkiplistMem) Delete(k string) error {
	return nil
}

func (s *SkiplistMem) Update(k string, n MemtableNode) error {
	return nil
}

func (s *SkiplistMem) Get(k string) MemtableNode {
	pN := s.head
	for i := s.maxLevel - 1; i >= 0; i-- {
		for pN.forwards[i] != nil && pN.forwards[i].key < k {
			pN = pN.forwards[i]
		}
	}
	if pN.forwards[0] != nil && pN.forwards[0].key == k {
		return pN.forwards[0]
	}
	return nil
}

func (s *SkiplistMem) Display() []MemtableNode {
	return nil
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
	for i := 0; rand.Int(n) == 1 && i < s.height; i++ {
		level++
	}
	return level
}
