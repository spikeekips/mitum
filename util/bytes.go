package util

type Byter interface {
	Bytes() []byte
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
