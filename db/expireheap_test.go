package db

import "testing"

func Test_NewKeyExpireHeap(t *testing.T) {
	h := newKeyExpireHeap(100)
	t.Logf("key expire heap's keyExpires len: %d, cap: %d", len(h.keyExpires), cap(h.keyExpires))
	t.Logf("key expire heap size: %d, cap: %d", h.size, h.cap)
}

func Test_Push(t *testing.T) {
	h := newKeyExpireHeap(10)
	en := &keyExpire{
		key:     "k0",
		seconds: 10,
	}
	en1 := &keyExpire{
		key:     "k1",
		seconds: 9,
	}
	en2 := &keyExpire{
		key:     "k2",
		seconds: 8,
	}
	en3 := &keyExpire{
		key:     "k3",
		seconds: 7,
	}
	en4 := &keyExpire{
		key:     "k4",
		seconds: 6,
	}
	en5 := &keyExpire{
		key:     "k5",
		seconds: 5,
	}
	en6 := &keyExpire{
		key:     "k6",
		seconds: 4,
	}
	en7 := &keyExpire{
		key:     "k7",
		seconds: 3,
	}
	en8 := &keyExpire{
		key:     "k8",
		seconds: 2,
	}
	en9 := &keyExpire{
		key:     "k9",
		seconds: 1,
	}
	en10 := &keyExpire{
		key:     "k10",
		seconds: 0,
	}

	h.push(en)
	h.push(en1)
	h.push(en2)
	h.push(en3)
	h.push(en4)
	h.push(en5)
	h.push(en6)
	h.push(en7)
	h.push(en8)
	h.push(en9)
	h.push(en10)
	t.Logf("key expire heap top0: %v,", h.keyExpires[0])
	t.Logf("key expire heap top1: %v,", h.keyExpires[1])
	t.Logf("key expire heap top2: %v,", h.keyExpires[2])
	t.Logf("key expire heap top3: %v,", h.keyExpires[3])
	t.Logf("key expire heap top4: %v,", h.keyExpires[4])
	t.Logf("key expire heap top5: %v,", h.keyExpires[5])
	t.Logf("key expire heap top6: %v,", h.keyExpires[6])
	t.Logf("key expire heap top7: %v,", h.keyExpires[7])
	t.Logf("key expire heap top8: %v,", h.keyExpires[8])
	t.Logf("key expire heap top9: %v,", h.keyExpires[9])
	t.Logf("key expire heap top10: %v,", h.keyExpires[10])
	t.Logf("key expire heap size: %d,", h.size)
	t.Logf("key expire heap cap: %d,", h.cap)
}

func Test_Pop(t *testing.T) {

}
