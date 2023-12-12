package util

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/suite"
)

type testSetInterfaceValue struct {
	suite.Suite
}

func (t *testSetInterfaceValue) TestInterfaceValue() {
	t.Run("uint64", func() {
		var a uint64 = 3
		var b uint64

		t.NoError(SetInterfaceValue(a, &b))

		t.Equal(a, b)
	})

	t.Run("string", func() {
		var a string = UUID().String()
		var b string

		t.NoError(SetInterfaceValue(a, &b))

		t.Equal(a, b)
	})

	t.Run("different", func() {
		var a uint64 = 33
		var b string

		err := SetInterfaceValue(a, &b)
		t.ErrorContains(err, "expected")
		t.NotEqual(a, b)
	})

	t.Run("complex", func() {
		a := &url.URL{
			Scheme: "local",
			Host:   "a.b.c",
			Path:   "/d/e/f",
		}

		var b *url.URL

		t.NoError(SetInterfaceValue(a, &b))

		t.Equal(a, b)
		t.Equal(a.String(), b.String())
	})

	t.Run("interface", func() {
		a := &url.URL{
			Scheme: "local",
			Host:   "a.b.c",
			Path:   "/d/e/f",
		}

		var b fmt.Stringer

		t.NoError(SetInterfaceValue(a, &b))

		t.Equal(a, b)
		t.Equal(a.String(), b.String())
	})
}

func TestSetInterfaceValue(t *testing.T) {
	suite.Run(t, new(testSetInterfaceValue))
}
