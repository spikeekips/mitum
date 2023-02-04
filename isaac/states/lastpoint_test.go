package isaacstates

import (
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/stretchr/testify/suite"
)

func TestLastPointIsNewVoteproof(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	cases := []struct {
		name   string
		last   [5]interface{} // height, round, stage, isMajority, isSuffrageConfirm
		target [5]interface{} // height, round, stage, isMajority, isSuffrageConfirm
		result bool
	}{
		{
			name:   "higher height",
			last:   [5]interface{}{33, 0, "INIT", true, false},
			target: [5]interface{}{34, 0, "INIT", true, false},
			result: true,
		},
		{
			name:   "lower height",
			last:   [5]interface{}{33, 0, "INIT", true, false},
			target: [5]interface{}{32, 0, "INIT", true, false},
			result: false,
		},
		{
			name:   "same",
			last:   [5]interface{}{33, 0, "INIT", true, false},
			target: [5]interface{}{33, 0, "INIT", true, false},
			result: false,
		},
		{
			name:   "same height, lower round",
			last:   [5]interface{}{33, 1, "INIT", true, false},
			target: [5]interface{}{33, 0, "INIT", true, false},
			result: false,
		},
		{
			name:   "same height + higher round + last majority",
			last:   [5]interface{}{33, 0, "INIT", true, false},
			target: [5]interface{}{33, 1, "INIT", true, false},
			result: true,
		},
		{
			name:   "same height, higher round, last not majority",
			last:   [5]interface{}{33, 0, "INIT", false, false},
			target: [5]interface{}{33, 1, "INIT", true, false},
			result: true,
		},
		{
			name:   "same + last majority + majority",
			last:   [5]interface{}{33, 0, "INIT", true, false},
			target: [5]interface{}{33, 0, "INIT", true, false},
			result: false,
		},
		{
			name:   "same + last not majority + majority",
			last:   [5]interface{}{33, 0, "INIT", false, false},
			target: [5]interface{}{33, 0, "INIT", true, false},
			result: true,
		},
		{
			name:   "same height, lower stage",
			last:   [5]interface{}{33, 0, "ACCEPT", true, false},
			target: [5]interface{}{33, 0, "INIT", true, false},
			result: false,
		},
		{
			name:   "same height, higher stage",
			last:   [5]interface{}{33, 0, "INIT", true, false},
			target: [5]interface{}{33, 0, "ACCEPT", true, false},
			result: true,
		},
		{
			name:   "same + last suffrage confirm + suffrage confirm",
			last:   [5]interface{}{33, 0, "INIT", true, true},
			target: [5]interface{}{33, 0, "INIT", true, true},
			result: false,
		},
		{
			name:   "same + last not suffrage confirm + suffrage confirm",
			last:   [5]interface{}{33, 0, "INIT", true, false},
			target: [5]interface{}{33, 0, "INIT", true, true},
			result: true,
		},
		{
			name:   "lower stage; suffrage confirm",
			last:   [5]interface{}{33, 0, "ACCEPT", true, false},
			target: [5]interface{}{33, 0, "INIT", true, true},
			result: false,
		},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(c.name, func() {
			last, err := newLastPoint(
				base.NewStagePoint(
					base.RawPoint(
						int64(c.last[0].(int)),
						uint64(c.last[1].(int)),
					),
					base.Stage(c.last[2].(string)),
				),
				c.last[3].(bool),
				c.last[4].(bool),
			)
			t.NoError(err)

			targetpoint := base.NewStagePoint(
				base.RawPoint(
					int64(c.target[0].(int)),
					uint64(c.target[1].(int)),
				),
				base.Stage(c.target[2].(string)),
			)

			isMajority := c.target[3].(bool)
			isSuffrageConfirm := c.target[4].(bool)

			result := IsNewVoteproofbyPoint(
				last,
				targetpoint,
				isMajority,
				isSuffrageConfirm,
			)

			t.Equal(c.result, result, "%d: %s; last=%v, from=%v(isMajority=%v isSuffrageConfirm=%v), expected=%v actual=%v", i, c.name,
				last,
				targetpoint, isMajority, isSuffrageConfirm,
				c.result, result,
			)
		})
	}
}

func TestLastPointIsNewBallot(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	cases := []struct {
		name   string
		last   [5]interface{} // height, round, stage, isMajority
		target [5]interface{} // height, round, stage, isSuffrageConfirm
		result bool
	}{
		{
			name:   "higher height",
			last:   [5]interface{}{33, 0, "INIT", true, false},
			target: [5]interface{}{34, 0, "INIT", false},
			result: true,
		},
		{
			name:   "lower height",
			last:   [5]interface{}{33, 0, "INIT", true, false},
			target: [5]interface{}{32, 0, "INIT", false},
			result: false,
		},
		{
			name:   "same",
			last:   [5]interface{}{33, 0, "INIT", true, false},
			target: [5]interface{}{33, 0, "INIT", false},
			result: false,
		},
		{
			name:   "same height, lower round",
			last:   [5]interface{}{33, 1, "INIT", true, false},
			target: [5]interface{}{33, 0, "INIT", false},
			result: false,
		},
		{
			name:   "same height + higher round + last majority",
			last:   [5]interface{}{33, 0, "INIT", true, false},
			target: [5]interface{}{33, 1, "INIT", false},
			result: true,
		},
		{
			name:   "same height, higher round, last not majority",
			last:   [5]interface{}{33, 0, "INIT", false, false},
			target: [5]interface{}{33, 1, "INIT", false},
			result: true,
		},
		{
			name:   "same height, lower stage",
			last:   [5]interface{}{33, 0, "ACCEPT", true, false},
			target: [5]interface{}{33, 0, "INIT", false},
			result: false,
		},
		{
			name:   "same height, higher stage",
			last:   [5]interface{}{33, 0, "INIT", true, false},
			target: [5]interface{}{33, 0, "ACCEPT", false},
			result: true,
		},
		{
			name:   "same + last suffrage confirm; suffrage confirm",
			last:   [5]interface{}{33, 0, "INIT", true, true},
			target: [5]interface{}{33, 0, "INIT", true},
			result: false,
		},
		{
			name:   "same + last not suffrage confirm; suffrage confirm",
			last:   [5]interface{}{33, 0, "INIT", true, false},
			target: [5]interface{}{33, 0, "INIT", true},
			result: true,
		},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(c.name, func() {
			last, err := newLastPoint(
				base.NewStagePoint(
					base.RawPoint(
						int64(c.last[0].(int)),
						uint64(c.last[1].(int)),
					),
					base.Stage(c.last[2].(string)),
				),
				c.last[3].(bool),
				c.last[4].(bool),
			)
			t.NoError(err)

			targetpoint := base.NewStagePoint(
				base.RawPoint(
					int64(c.target[0].(int)),
					uint64(c.target[1].(int)),
				),
				base.Stage(c.target[2].(string)),
			)

			isSuffrageConfirm := c.target[3].(bool)
			result := isNewBallot(
				last,
				targetpoint,
				isSuffrageConfirm,
			)

			t.Equal(c.result, result, "%d: %s; last=%v, from=%v(isSuffrageConfirm=%v), expected=%v actual=%v", i, c.name,
				last,
				targetpoint, isSuffrageConfirm,
				c.result, result,
			)
		})
	}
}
