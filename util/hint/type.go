package hint

import (
	"regexp"

	"github.com/spikeekips/mitum/util"
)

var (
	reTypeAllowedChars           = regexp.MustCompile(`^[a-z0-9][a-z0-9\-_\+]*[a-z0-9]$`)
	minTypeLength, MaxTypeLength = 2, 100
)

type Type string // revive:disable-line:redefines-builtin-id

func (t Type) IsValid([]byte) error {
	switch n := len(t); {
	case n < minTypeLength:
		return util.InvalidError.Errorf("empty Type")
	case n > MaxTypeLength:
		return util.InvalidError.Errorf("Type too long")
	}

	if !reTypeAllowedChars.Match([]byte(t)) {
		return util.InvalidError.Errorf("invalid char found in Type")
	}

	return nil
}

func (t Type) Bytes() []byte {
	return []byte(t)
}

func (t Type) String() string {
	return string(t)
}

func ParseFixedTypedString(s string, typesize int) (string, Type, error) {
	if len(s) <= typesize {
		return "", Type(""), util.InvalidError.Errorf("too short fixed typed string, %q", s)
	}

	return s[:len(s)-typesize], Type(s[len(s)-typesize:]), nil
}
