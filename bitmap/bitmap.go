package bitmap

type Bitmap struct {
	size int
	bits []uint64
}

func NewBitmap(size int) *Bitmap {
	return &Bitmap{
		size: size,
		bits: make([]uint64, size/64+1),
	}
}

func (b *Bitmap) Set(n int) {
	if n > b.size || n < 0 {
		return
	}

	b.bits[n/b.size] |= 1 << (n % 64)
}

func (b *Bitmap) UnSet(n int) {
	if n > b.size || n < 0 {
		return
	}

	mask := ^(uint64(1) << (n % 64))
	b.bits[n/b.size] = mask & b.bits[n/b.size]
}

func (b *Bitmap) Get(n int) bool {
	if n > b.size || n < 0 {
		return false
	}
	bucket := b.bits[n/b.size]

	return bucket&(1<<(n%64)) != 0
}
