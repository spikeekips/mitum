package launch

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"golang.org/x/time/rate"
)

func (r RateLimiterRule) MarshalText() ([]byte, error) {
	switch {
	case r.Limit == rate.Inf:
		return []byte("nolimit"), nil
	case r.Limit == 0, r.Burst < 1:
		return []byte("0"), nil
	default:
		return []byte(fmt.Sprintf("%d/%s",
			r.Burst,
			time.Duration(float64(1/r.Limit)*float64(time.Second)).String(),
		)), nil
	}
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
			limit = rate.Every(dur)
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

	return nil
}

func (rs NetRateLimiterRuleSet) MarshalJSON() ([]byte, error) {
	if len(rs.ipnets) < 1 {
		return nil, nil
	}

	n := make([]map[string]map[string]RateLimiterRule, len(rs.ipnets))

	for i := range rs.ipnets {
		ip := rs.ipnets[i]
		n[i] = map[string]map[string]RateLimiterRule{
			ip.String(): rs.rules[ip.String()],
		}
	}

	return util.MarshalJSON(n)
}

func (rs *NetRateLimiterRuleSet) UnmarshalJSON(b []byte) error {
	var u []map[string]map[string]RateLimiterRule
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	if len(u) < 1 {
		return nil
	}

	rs.rules = map[string]map[string]RateLimiterRule{}
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

func (rs NodeRateLimiterRuleSet) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(rs.rules)
}

func (rs *NodeRateLimiterRuleSet) UnmarshalJSON(b []byte) error {
	return util.UnmarshalJSON(b, &rs.rules)
}

func (rs *SuffrageRateLimiterRuleSet) MarshalJSON() ([]byte, error) {
	m := map[string]RateLimiterRule{}
	if rs.d.Limit > 0 {
		m["default"] = rs.d
	}

	for i := range rs.rules {
		m[i] = rs.rules[i]
	}

	return util.MarshalJSON(m)
}

func (rs *SuffrageRateLimiterRuleSet) UnmarshalJSON(b []byte) error {
	var u map[string]RateLimiterRule
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	if len(u) < 1 {
		return nil
	}

	rs.rules = map[string]RateLimiterRule{}

	for i := range u {
		if i == "default" {
			rs.d = u[i]

			continue
		}

		rs.rules[i] = u[i]
	}

	return nil
}
