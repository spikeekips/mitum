package util

type Byter interface {
	Bytes() []byte
}

func ConcatByters(bs ...Byter) []byte {
	b := make([][]byte, len(bs))
	for i := range bs {
		j := bs[i]
		if j == nil {
			continue
		}
		b[i] = j.Bytes()
	}

	return ConcatBytesSlice(b...)
}

func ConcatBytesSlice(sl ...[]byte) []byte {
	var t int
	for _, s := range sl {
		t += len(s)
	}

	n := make([]byte, t)
	var i int
	for _, s := range sl {
		i += copy(n[i:], s)
	}

	return n
}
