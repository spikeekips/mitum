package hint

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testBaseHinter struct {
	suite.Suite
}

func (t *testBaseHinter) TestNew() {
	b := NewBaseHinter(MustNewHint("abc-v1.2.3"))
	t.NoError(b.IsValid(nil))
}

func (t *testBaseHinter) TestIsValidWithType() {
	b := NewBaseHinter(MustNewHint("abc-v1.2.3"))
	t.NoError(b.IsValid(b.Hint().Type().Bytes()))
}

func (t *testBaseHinter) TestIsValidWithUnknownType() {
	b := NewBaseHinter(MustNewHint("abc-v1.2.3"))
	err := b.IsValid(Type("showme").Bytes())
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "type does not match")
}

func TestBaseHinter(t *testing.T) {
	suite.Run(t, new(testBaseHinter))
}
