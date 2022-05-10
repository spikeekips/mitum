package hint

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

var (
	MaxVersionLength = 20
	MaxHintLength    = MaxTypeLength + MaxVersionLength + 1
	regVersion       = regexp.MustCompile(`\-v\d+`)
)

type Hinter interface {
	Hint() Hint
}

type Hint struct {
	t Type
	v util.Version
}

func NewHint(t Type, v util.Version) Hint {
	return Hint{t: t, v: v}
}

// EnsureParseHint tries to parse hint string, but skips to check IsValid().
func EnsureParseHint(s string) Hint {
	l := regVersion.FindStringIndex(s)
	if len(l) < 1 {
		return Hint{}
	}

	return NewHint(Type(s[:l[0]]), util.EnsureParseVersion(s[l[0]+1:]))
}

// ParseHint tries to parse hint string and also checks IsValid().
func ParseHint(s string) (Hint, error) {
	ns := strings.TrimSpace(s)

	if l := regVersion.FindStringIndex(ns); len(l) < 1 {
		return Hint{}, util.ErrInvalid.Errorf("invalid hint string, %q", ns)
	}

	ht := EnsureParseHint(ns)
	if err := ht.IsValid(nil); err != nil {
		return Hint{}, errors.Wrap(err, "failed to parse hint")
	}

	return ht, nil
}

func MustNewHint(s string) Hint {
	ht, err := ParseHint(s)
	if err != nil {
		panic(err)
	}

	return ht
}

func (ht Hint) IsValid([]byte) error {
	if err := ht.t.IsValid(nil); err != nil {
		return errors.Wrap(err, "invalid type in hint")
	}

	if err := ht.v.IsValid(nil); err != nil {
		return errors.Wrap(err, "invalid version in hint")
	}

	if l := len(ht.v.String()); l > MaxVersionLength {
		return util.ErrInvalid.Errorf("too long version in hint, %d > %d", l, MaxVersionLength)
	}

	return nil
}

func (ht Hint) Type() Type {
	return ht.t
}

func (ht Hint) Version() util.Version {
	return ht.v
}

func (ht Hint) Bytes() []byte {
	return []byte(ht.String())
}

func (ht Hint) String() string {
	return fmt.Sprintf("%s-%s", ht.t, ht.v)
}

func (ht Hint) Equal(b Hint) bool {
	return ht.t == b.t && ht.v.Compare(b.v) == 0
}

// IsCompatible checks whether target is compatible with source. Obviously, Type
// should be same and version is compatible.
func (ht Hint) IsCompatible(b Hint) bool {
	if ht.t != b.t {
		return false
	}

	return ht.v.IsCompatible(b.v)
}

func (ht Hint) IsEmpty() bool {
	return len(ht.t) < 1 || ht.v.IsEmpty()
}

func (ht Hint) MarshalText() ([]byte, error) {
	return []byte(ht.String()), nil
}

func (ht *Hint) UnmarshalText(b []byte) error {
	*ht = EnsureParseHint(string(b))

	return nil
}
