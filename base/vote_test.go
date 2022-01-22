package base

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindMajority(t *testing.T) {
	cases := []struct {
		name      string
		quorum    uint
		threshold uint
		set       []uint
		expected  int
	}{
		{
			name:   "threshold > quorum; yes",
			quorum: 10, threshold: 20,
			set:      []uint{10, 0},
			expected: 0,
		},
		{
			name:   "threshold > quorum; nop",
			quorum: 10, threshold: 20,
			set:      []uint{0, 10},
			expected: 1,
		},
		{
			name:   "not yet",
			quorum: 10, threshold: 7,
			set:      []uint{1, 1},
			expected: -1,
		},
		{
			name:   "yes",
			quorum: 10, threshold: 7,
			set:      []uint{7, 1},
			expected: 0,
		},
		{
			name:   "#2",
			quorum: 10, threshold: 7,
			set:      []uint{0, 2, 7},
			expected: 2,
		},
		{
			name:   "nop",
			quorum: 10, threshold: 7,
			set:      []uint{1, 7},
			expected: 1,
		},
		{
			name:   "not draw #0",
			quorum: 10, threshold: 7,
			set:      []uint{3, 3},
			expected: -1,
		},
		{
			name:   "not draw #1",
			quorum: 10, threshold: 7,
			set:      []uint{0, 4},
			expected: -1,
		},
		{
			name:   "draw #0",
			quorum: 10, threshold: 7,
			set:      []uint{4, 4},
			expected: -2,
		},
		{
			name:   "draw #1",
			quorum: 10, threshold: 7,
			set:      []uint{5, 5},
			expected: -2,
		},
		{
			name:   "draw #2",
			quorum: 10, threshold: 7,
			set:      []uint{3, 3, 3},
			expected: -2,
		},
		{
			name:   "over quorum",
			quorum: 10, threshold: 17,
			set:      []uint{4, 4},
			expected: -2,
		},
		{
			name:   "1 quorum 1 threshold",
			quorum: 1, threshold: 1,
			set:      []uint{1, 0},
			expected: 0,
		},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(
			c.name,
			func(*testing.T) {
				result := FindMajority(c.quorum, c.threshold, c.set...)
				assert.Equal(t, c.expected, result, "%d: %v; %v != %v", i, c.name, c.expected, result)
			},
		)
	}
}

func TestFindVoteResult(t *testing.T) {
	cases := []struct {
		name      string
		quorum    uint
		threshold uint
		s         []string
		expected  string
		result    VoteResult
	}{
		{
			name:   "over threshold",
			quorum: 3, threshold: 2,
			s:        []string{"c", "a", "a"},
			expected: "a",
			result:   VoteResultMajority,
		},
		{
			name:   "draw",
			quorum: 3, threshold: 2,
			s:        []string{"c", "a", "b"},
			expected: "",
			result:   VoteResultDraw,
		},
		{
			name:   "not yet",
			quorum: 3, threshold: 2,
			s:        []string{"c", "a"},
			expected: "",
			result:   VoteResultNotYet,
		},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(
			c.name,
			func(*testing.T) {
				result, key := FindVoteResult(c.quorum, c.threshold, c.s)
				assert.Equal(t, c.expected, key, "%d: %v; %v != %v", i, c.name, c.expected, key)
				assert.Equal(t, c.result, result, "%d: %v; %v != %v", i, c.name, c.expected, result)
			},
		)
	}
}
