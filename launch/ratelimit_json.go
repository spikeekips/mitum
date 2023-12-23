package launch

import (
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"golang.org/x/time/rate"
)

func (r *RateLimiter) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		Limiter   string
		Type      string
		Checksum  string
		Desc      string
		UpdatedAt int64
		NoLimit   bool
	}{
		Limiter:   humanizeRateLimiter(r.Limit(), r.Burst()),
		Type:      r.t,
		Checksum:  r.checksum,
		Desc:      r.desc,
		UpdatedAt: r.updatedAt,
		NoLimit:   r.nolimit,
	})
}

func (r RateLimiterRule) String() string {
	if len(r.s) > 0 {
		return r.s
	}

	b, _ := r.MarshalText()

	return string(b)
}

func (r RateLimiterRule) MarshalText() ([]byte, error) {
	if len(r.s) > 0 {
		return []byte(r.s), nil
	}

	return []byte(humanizeRateLimiter(r.Limit, r.Burst)), nil
}

var timeUnits = map[string]struct{}{
	"ns": {},
	"us": {},
	"µs": {},
	"μs": {},
	"ms": {},
	"s":  {},
	"m":  {},
	"h":  {},
}

func (r *RateLimiterRule) UnmarshalText(b []byte) error {
	n := strings.TrimSpace(string(b))

	var limit rate.Limit
	var burst int

	switch {
	case len(n) < 1:
		return errors.Errorf("empty")
	case n == "nolimit":
		limit = rate.Inf
	case n == "0":
	default:
		i := strings.SplitN(n, "/", 2)
		if len(i) != 2 {
			return errors.Errorf(`invalid format RateLimiterRule; "{0 | nolimit | <burst>/<duration>}"`)
		}

		if _, found := timeUnits[i[1]]; found {
			i[1] = "1" + i[1]
		}

		switch i, err := strconv.ParseInt(i[0], 10, 64); {
		case err != nil:
			return errors.Wrap(err, "burst")
		default:
			burst = int(i)
		}

		switch dur, err := time.ParseDuration(i[1]); {
		case err != nil:
			return errors.Wrap(err, "duration")
		case dur < 1:
		default:
			limit = makeLimit(dur, burst)
		}
	}

	switch {
	case limit == rate.Inf:
		r.Limit = rate.Inf
		r.Burst = 0
	case limit == 0, burst < 1:
		r.Limit = 0
		r.Burst = 0
	default:
		r.Limit = limit
		r.Burst = burst
	}

	r.s = n

	return nil
}

func (m RateLimiterRuleMap) MarshalJSON() ([]byte, error) {
	n := map[string]RateLimiterRule{}
	defer clear(n)

	for i := range m.m {
		n[i] = m.m[i]
	}

	if m.d != nil {
		n["default"] = *m.d
	}

	return util.MarshalJSON(n)
}

func (m *RateLimiterRuleMap) UnmarshalJSON(b []byte) error {
	var u map[string]RateLimiterRule
	defer clear(u)

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	m.d = nil
	m.m = map[string]RateLimiterRule{}

	for i := range u {
		if i == "default" {
			d := u[i]
			m.d = &d

			continue
		}

		m.m[i] = u[i]
	}

	return nil
}

func (rs NetRateLimiterRuleSet) MarshalJSON() ([]byte, error) {
	if len(rs.ipnets) < 1 {
		return nil, nil
	}

	n := make([]map[string]RateLimiterRuleMap, len(rs.ipnets))
	defer func() {
		for i := range n {
			clear(n[i])
		}

		clear(n)
	}()

	for i := range rs.ipnets {
		ip := rs.ipnets[i]
		n[i] = map[string]RateLimiterRuleMap{
			ip.String(): rs.rules[ip.String()],
		}
	}

	return util.MarshalJSON(n)
}

func (rs *NetRateLimiterRuleSet) UnmarshalJSON(b []byte) error {
	var u []map[string]RateLimiterRuleMap
	defer clear(u)

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	if len(u) < 1 {
		return nil
	}

	rs.rules = map[string]RateLimiterRuleMap{}
	rs.ipnets = make([]*net.IPNet, len(u))

	for i := range u {
		for j := range u[i] {
			_, ipnet, err := net.ParseCIDR(j)
			if err != nil {
				return errors.Wrap(err, "ipnet")
			}

			rs.ipnets[i] = ipnet
			rs.rules[ipnet.String()] = u[i][j]
		}
	}

	return nil
}

func (rs StringKeyRateLimiterRuleSet) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(rs.rules)
}

func (rs *StringKeyRateLimiterRuleSet) UnmarshalJSON(b []byte) error {
	return util.UnmarshalJSON(b, &rs.rules)
}

func (rs *SuffrageRateLimiterRuleSet) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(rs.rules)
}

func (rs *SuffrageRateLimiterRuleSet) UnmarshalJSON(b []byte) error {
	return util.UnmarshalJSON(b, &rs.rules)
}
