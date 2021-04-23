package memtable

import (
	"testing"
)

func TestNewSkiplistNode(t *testing.T) {
	v := []byte{'v', '1'}
	n := NewSkiplistNode("k1", v, -1, 0, 0)
	t.Logf("test func NewSkiplistNode ===> n key: %v, value: %v, expireAt: %v, level: %v\n", n.key, n.value, n.expireAt, n.level)
	t.Logf("test method *SkiplistNode.Key, *SkiplistNode.Value, *SkiplistNode.ExpireAt  ===> n key: %v, value: %v, expireAt: %v\n", n.Key(), n.Value(), n.ExpireAt())
	v1 := []byte{'v', '2'}
	n1 := NewSkiplistNode("k2", v1, -1, 0, 0)
	t.Logf("test method *SkiplistNode.Less ===> n < n1: %v", n.Less(n1))

	// e is an expired timestamp
	n2 := NewSkiplistNode("k3", v1, 1587631962, 0, 0)
	t.Logf("test func NewSkiplistNode ===> n2 key: %v, expireAt: %v\n", n2.key, n2.expireAt)
	// e is a future timestamp
	n3 := NewSkiplistNode("k4", v1, 1650703962, 0, 0)
	t.Logf("test func NewSkiplistNode ===> n3 key: %v, expireAt: %v\n", n3.key, n3.expireAt)

	// l > 0
	n4 := NewSkiplistNode("k5", v1, 1650703962, 100000, 0)
	t.Logf("test func NewSkiplistNode ===> n4 key: %v, level: %v\n", n4.key, n4.level)
	// l < 0
	n5 := NewSkiplistNode("k6", v1, 1650703962, -1, 0)
	t.Logf("test func NewSkiplistNode ===> n5 key: %v, level: %v\n", n5.key, n5.level)

	// fl > 0
	n6 := NewSkiplistNode("k7", v1, 1650703962, 100000, 20)
	t.Logf("test func NewSkiplistNode ===> n6 key: %v, len(forwards): %v\n", n6.key, len(n6.forwards))
	// fl <= 0
	n7 := NewSkiplistNode("k8", v1, 1650703962, -1, 0)
	t.Logf("test func NewSkiplistNode ===> n7 key: %v, len(forwards): %v\n", n7.key, len(n7.forwards))
}

func TestNewSkiplistMem(t *testing.T) {
	// h > 0, lp <= 0
	s := NewSkiplistMem(10, 0)
	t.Logf("test func NewSkiplistMem ===> s height: %v, maxLevel: %v, levelP: %v \n", s.height, s.maxLevel, s.levelP)
	// h <= 0
	s1 := NewSkiplistMem(-1, 0)
	t.Logf("test func NewSkiplistMem ===> s1 height: %v, maxLevel: %v, levelP: %v \n", s1.height, s1.maxLevel, s1.levelP)
	// lp >= 1
	s2 := NewSkiplistMem(10, 1)
	t.Logf("test func NewSkiplistMem ===> s2 height: %v, maxLevel: %v, levelP: %v \n", s2.height, s2.maxLevel, s2.levelP)
}

func TestInsert(t *testing.T) {
	s := NewSkiplistMem(0, 0)
	s.Insert("k1", []byte{}, 1650703962)
}
