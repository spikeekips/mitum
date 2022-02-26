package leveldbstorage

func copyBytes(b []byte) []byte {
	n := make([]byte, len(b))
	copy(n, b)

	return b
}
