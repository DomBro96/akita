package memtable

// MemtableNode is an interface represents the Node of Memtable
type MemtableNode interface {
	Key() string
	Value() []byte
	ExpireAt() int64
	Less(MemtableNode) bool
}

// Memtable is an interface that represents the memory structure in LSM
type Memtable interface {
	Insert(MemtableNode) error
	Delete(string) error
	Update(string, MemtableNode) error
	Get(string) MemtableNode
	Display() []MemtableNode
	Full() bool
	Size() int
	Usage() int
	Limit() int
}
