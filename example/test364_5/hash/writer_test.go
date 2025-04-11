package hash

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestHex64Writer(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	t.Run("ok", func() {
		b := []byte{0, 1, 2, 3, 4}

		expected := NewSHA3(b)

		w, s := NewHex64Writer()
		w(b)
		h := s()

		t.Equal(expected.String(), h.String())
	})

	t.Run("empty", func() {
		expected := NewSHA3(nil)

		_, s := NewHex64Writer()
		h := s()

		t.Equal(expected.String(), h.String())
	})
}
