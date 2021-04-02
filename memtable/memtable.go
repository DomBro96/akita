package memtable

type Memtable interface {
	Insert(string, []byte) error
	Delete(string) error
	Update(string) error
	Get(string) []byte
}
