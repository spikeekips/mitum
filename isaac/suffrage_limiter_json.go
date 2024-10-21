package isaac

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type fixedSuffrageCandidateLimiterRuleJSONMarshaler struct {
	hint.BaseHinter
	Limit uint64 `json:"limit"`
}
type fixedSuffrageCandidateLimiterRuleJSONUnmarshaler struct {
	Limit uint64 `json:"limit"`
}

func (l FixedSuffrageCandidateLimiterRule) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(fixedSuffrageCandidateLimiterRuleJSONMarshaler{
		BaseHinter: l.BaseHinter,
		Limit:      l.limit,
	})
}

func (l *FixedSuffrageCandidateLimiterRule) UnmarshalJSON(b []byte) error {
	var u fixedSuffrageCandidateLimiterRuleJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.WithMessage(err, "unmarshal FixedSuffrageCandidateLimiterRule")
	}

	l.limit = u.Limit

	return nil
}

type majoritySuffrageCandidateLimiterRuleJSONMarshaler struct {
	hint.BaseHinter
	Ratio float64 `json:"ratio"`
	Min   uint64  `json:"min"`
	Max   uint64  `json:"max"`
}

type majoritySuffrageCandidateLimiterRuleJSONUnmarshaler struct {
	Ratio float64 `json:"ratio"`
	Min   uint64  `json:"min"`
	Max   uint64  `json:"max"`
}

func (l MajoritySuffrageCandidateLimiterRule) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(majoritySuffrageCandidateLimiterRuleJSONMarshaler{
		BaseHinter: l.BaseHinter,
		Ratio:      l.ratio,
		Min:        l.minv,
		Max:        l.maxv,
	})
}

func (l *MajoritySuffrageCandidateLimiterRule) UnmarshalJSON(b []byte) error {
	var u majoritySuffrageCandidateLimiterRuleJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.WithMessage(err, "unmarshal MajoritySuffrageCandidateLimiterRule")
	}

	l.ratio = u.Ratio
	l.minv = u.Min
	l.maxv = u.Max

	return nil
}
