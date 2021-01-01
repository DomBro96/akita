package common

// BytePool implements a leaky pool of []byte in the form of a bounded
type BytePool struct {
	c     chan []byte
	s     int // len of c
	cap   int
	clean bool
}

// NewBytePool create a new BytePool
func NewBytePool(s int, cap int, clean bool) *BytePool {
	return &BytePool{
		c:     make(chan []byte, s),
		cap:   cap,
		clean: clean,
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
	case p.c <- b:
	default:
		// just discard
	}
}

// Clean represent a []byte need to clen when put it to BytePool
func (p *BytePool) Clean() bool {
	return p.clean
}
