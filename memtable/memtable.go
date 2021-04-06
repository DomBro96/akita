package memtable

import "akita/db"

type Memtable interface {
	Insert(string, []byte) error
	Delete(string) error
	Update(string) error
	Get(string) []byte
	Display() []*db.DataRecord
	Full() bool
	Size() int
	SizeOfUsage() int
}
