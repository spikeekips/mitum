package util

import (
	"regexp"
	"strings"
)

type Stringer func() string

func (s Stringer) String() string {
	return s()
}

var rWhitespace = regexp.MustCompile(`\s`)

func DelmSplitStrings(s, delm string, n uint) string {
	switch {
	case len(s) < 1:
		return s
	case rWhitespace.MatchString(s):
		s = string(rWhitespace.ReplaceAll([]byte(s), []byte("_"))) //revive:disable-line:modifies-parameter
	}

	var sb strings.Builder

	var i uint

	for {
		e := (i * n) + n //nolint:mnd //...
		if e > uint(len(s)) {
			e = uint(len(s))
		}

		j := s[i*n : e]
		if len(j) < 1 {
			break
		}

		if i > 0 {
			_, _ = sb.WriteString(delm)
		}

		_, _ = sb.WriteString(j)

		if uint(len(j)) < n { //nolint:mnd //...
			break
		}

		i++
	}

	return sb.String()
}
