package hint

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/spikeekips/mitum/util"
)

func HintFromJSONMarshaled(b []byte) (Hint, error) {
	var h struct {
		H Hint `json:"_hint"`
	}

	if err := util.JSON.Unmarshal(b, &h); err != nil {
		return Hint{}, err
	}

	return h.H, nil
}

type fieldHinted struct {
	H Hint `json:"_hint"`
	A int
	B string
}

func (fh fieldHinted) Hint() Hint {
	return fh.H
}

type testFeildHinted struct {
	suite.Suite
}

func (t *testFeildHinted) TestNew() {
	h, err := NewHint(Type{0xff, 0x10}, "0.0.1")
	t.NoError(err)

	fh := fieldHinted{
		H: h,
		A: 10,
		B: "showme",
	}

	t.Implements((*Hinter)(nil), fh)
}

func (t *testFeildHinted) TestHintFromJSONMarshaled() {
	h, err := NewHint(Type{0xff, 0x10}, "0.0.1")
	t.NoError(err)

	// NOTE to marshal Hint, especially Type, it's Type should be registered
	// before.
	_ = registerType(h.Type(), "0xff00-v0.0.1")

	fh := fieldHinted{
		H: h,
		A: 10,
		B: "showme",
	}

	b, err := util.JSON.Marshal(fh)
	t.NoError(err)
	t.NotNil(b)

	uh, err := HintFromJSONMarshaled(b)
	t.NoError(err)

	t.Equal(h.Type(), uh.Type())
	t.Equal(h.Version(), uh.Version())
}

func TestFeildHinted(t *testing.T) {
	suite.Run(t, new(testFeildHinted))
}
