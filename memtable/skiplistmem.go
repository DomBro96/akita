package memtable

var _ MemtableNode = new(SkiplistMemNode)
var _ Memtable = new(SkiplistMem)

// SkiplistMemNode implements MemtableNode which as SkiplistMem‘s Node.
type SkiplistMemNode struct {
	key      string
	value    []byte
	expireAt int64
	level    []*SkiplistMemNode
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
	size  int
	usage int
	limit int
}

func (s *SkiplistMem) Insert(n MemtableNode) error {
	return nil
}

func (s *SkiplistMem) Delete(key string) error {
	return nil
}

func (s *SkiplistMem) Update(key string, n MemtableNode) error {
	return nil
}

func (s *SkiplistMem) Get(key string) MemtableNode {
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
