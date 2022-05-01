package base

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestThreshold(t *testing.T) {
	cases := []struct {
		name      string
		quorum    uint
		threshold float64
		expected  uint // expected Threshold.Threshold
		err       string
	}{
		{
			name:      "0 quorum",
			quorum:    10,
			threshold: 67,
			err:       "0 quorum",
		},
		{
			name:      "under zero threshold: 0",
			quorum:    10,
			threshold: 0,
			err:       "under zero threshold",
		},
		{
			name:      "0 threshold: under 1",
			quorum:    10,
			threshold: 0.5,
			err:       "risky threshold",
		},
		{
			name:      "0 threshold: under 67",
			quorum:    10,
			threshold: 66,
			err:       "risky threshold",
		},
		{
			name:      "over threshold",
			quorum:    10,
			threshold: 100.5,
			err:       "over 100 threshold",
		},
		{
			name:      "threshold #0",
			quorum:    10,
			threshold: 50,
			expected:  5,
		},
		{
			name:      "ceiled #0",
			quorum:    10,
			threshold: 55,
			expected:  6,
		},
		{
			name:      "ceiled #1",
			quorum:    10,
			threshold: 51,
			expected:  6,
		},
		{
			name:      "ceiled #1",
			quorum:    10,
			threshold: 99,
			expected:  10,
		},
		{
			name:      "ceiled #1",
			quorum:    10,
			threshold: 67,
			expected:  7,
		},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(
			c.name,
			func(*testing.T) {
				tr := Threshold(c.threshold)
				err := tr.IsValid(nil)
				if len(c.err) > 0 {
					if err == nil {
						assert.Error(t, errors.Errorf("expected error: %s, but nothing happened", c.err), "%d: %v", i, c.name)
						return
					}

					assert.ErrorContains(t, err, c.err, "%d: %v", i, c.name)
					return
				}

				th := tr.Threshold(c.quorum)
				assert.Equal(t, c.expected, th, "%d: %v; %v != %v", i, c.name, c.expected, th)
			},
		)
	}
}

func TestNumberOfFaultyNodes(t *testing.T) {
	cases := []struct {
		name      string
		n         uint
		threshold float64
		expected  int
	}{
		{
			name:      "3, 67",
			n:         3,
			threshold: 67,
			expected:  0,
		},
		{
			name:      "3, 60",
			n:         3,
			threshold: 60,
			expected:  1,
		},
		{
			name:      "0, 60",
			n:         0,
			threshold: 60,
			expected:  0,
		},
		{
			name:      "10, 60",
			n:         10,
			threshold: 60,
			expected:  4,
		},
		{
			name:      "10, 61",
			n:         10,
			threshold: 61,
			expected:  3,
		},
		{
			name:      "10, 100",
			n:         10,
			threshold: 100,
			expected:  0,
		},
		{
			name:      "33, 200",
			n:         33,
			threshold: 200,
			expected:  0,
		},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(
			c.name,
			func(*testing.T) {
				f := NumberOfFaultyNodes(c.n, c.threshold)
				assert.Equal(t, c.expected, f, "%d: %v; %v != %v", i, c.name, c.expected, f)
			},
		)
	}
}
