package bloomfilter

import "akita/bitmap"

type BloomFilter struct {
	size      int
	hashCount int
	seeds     []int
	bitMap    *bitmap.Bitmap
}

func NewBloomFilter(size int) *BloomFilter {
	hc := size * 69 / 100 // 0.69 =~ ln(2)
	if hc < 1 {
		hc = 1
	} else if hc > 30 {
		hc = 30
	}

	primes := sievePrime(200)

	return &BloomFilter{
		size:      size,
		hashCount: hc,
		seeds:     primes[:hc],
		bitMap:    bitmap.NewBitmap(size),
	}
}

func (b *BloomFilter) Add(data []byte) {
	for _, seed := range b.seeds {
		h := simpleHash(data, b.size, seed)
		b.bitMap.Set(h)
	}
}

func (b *BloomFilter) Contain(data []byte) bool {
	has := true
	for _, seed := range b.seeds {
		h := simpleHash(data, b.size, seed)
		has = b.bitMap.Get(h)
		if !has {
			break
		}
	}

	return has
}

func simpleHash(data []byte, size int, seed int) int {
	res := 0
	for _, r := range data {
		res = seed*res + int(r)
	}
	return (size - 1) & res
}

// sievePrime filter prime numbers up to n
func sievePrime(n int) []int {
	if n <= 0 {
		return nil
	}

	res := make([]int, 0)

	isPrime := make([]bool, n+1)
	for i := 2; i < len(isPrime); i++ {
		isPrime[i] = true
	}

	for i := 2; i <= n; i++ {
		if !isPrime[i] {
			continue
		}

		res = append(res, i)

		for j := 2 * i; j <= n; j += i {
			isPrime[j] = false
		}

	}

	return res
}
