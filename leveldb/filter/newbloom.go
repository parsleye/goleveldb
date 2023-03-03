package filter

import "bytes"

type BloomFilter int

func (bf BloomFilter) Contains(filter, key []byte) bool {
	nBytes := len(filter) - 1
	nBits := nBytes * 8
	k := filter[nBytes]

	kh := bloomHash(key)
	delta := kh<<17 | kh>>15
	for i := uint8(0); i < k; i++ {
		bitPos := kh % uint32(nBits)
		x := 1 << bitPos % 8
		if int(filter[bitPos/8])&x == 0 {
			return false
		}
		kh += delta
	}

	return true
}

func (bf BloomFilter) NewGenerator() *Generator {
	k := uint8(bf * 69 / 100) // 0.69 =~ ln(2)
	if k < 1 {
		k = 1
	} else if k > 30 {
		k = 30
	}
	return &Generator{
		n: int(bf),
		k: k,
	}
}

type Generator struct {
	n         int
	k         uint8
	keyHashes []uint32
}

func (g *Generator) Add(key []byte) {
	g.keyHashes = append(g.keyHashes, bloomHash(key))
}

func (g *Generator) Generate(buf bytes.Buffer) {
	nBits := len(g.keyHashes) * g.n
	if nBits < 64 {
		nBits = 64
	}
	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8

	buf.Grow(nBytes + 1)
	to := buf.Next(nBytes + 1)
	to[nBytes] = g.k // k is small enough to fit into an uint8

	for _, kh := range g.keyHashes {
		delta := kh<<17 | kh>>15
		for i := uint8(0); i < g.k; i++ {
			bitPos := kh % uint32(nBits)
			to[bitPos/8] |= 1 << bitPos % 8
			kh += delta
		}
	}
}
