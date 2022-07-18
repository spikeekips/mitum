package hint

import (
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
)

func TestTypeIsValid(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	cases := []struct {
		name string
		s    string
		err  string
	}{
		{name: "valid", s: "showme"},
		{name: "blank head", s: " showme", err: "invalid char found"},
		{name: "blank tail", s: "showme ", err: "invalid char found"},
		{name: "uppercase", s: "shOwme", err: "invalid char found"},
		{name: "slash", s: "sh/wme", err: "invalid char found"},
		{name: "-v", s: "showme-ver"},
		{name: "-v0.", s: "showme-v0.1", err: "invalid char found"},
		{name: "hyphen", s: "sh-wme"},
		{name: "underscore", s: "sh-w_me"},
		{name: "plus", s: "sh-w_m+e"},
		{name: "empty", s: "", err: "too short Type"},
		{name: "too long", s: strings.Repeat("a", MaxTypeLength+1), err: "too long Type"},
		{name: "2 chars", s: "sa"},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(c.name, func() {
			ty := Type(c.s)
			err := ty.IsValid(nil)
			if len(c.err) > 0 {
				if err == nil {
					t.NoError(errors.Errorf("expected %q, but nil error", c.err), "%d: %v", i, c.name)

					return
				}

				t.ErrorContains(err, c.err, "%d: %v", i, c.name)
			} else if err != nil {
				t.NoError(errors.Errorf("expected nil error, but %+v", err), "%d: %v", i, c.name)

				return
			}
		})
	}
}
