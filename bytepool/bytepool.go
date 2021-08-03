package bytepool

// BytePool implements a leaky pool of []byte in the form of a bounded
type BytePool struct {
	c   chan []byte
	s   int // len of c
	cap int
}

// NewBytePool create a new BytePool
func NewBytePool(s int, cap int) *BytePool {
	return &BytePool{
		c:   make(chan []byte, s),
		cap: cap,
	}
}

// Get get a []byte from BytePool
func (p *BytePool) Get() (b []byte) {
	select {
	case b = <-p.c:
	default:
		b = make([]byte, 0, p.cap)
	}
	return
}

// Put give back a []byte to BytePool
func (p *BytePool) Put(b []byte) {
	select {
	case p.c <- b[:0]:
	default:
		// just discard
	}
}
