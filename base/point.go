package base

import (
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

var (
	NilHeight     = Height(-1)
	GenesisHeight = Height(0)
)

// Height stands for height of Block
type Height int64

func NewHeightFromString(s string) (Height, error) {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return NilHeight, errors.Wrap(err, "failed to NewHeightFromString")
	}

	return Height(i), nil
}

func NewHeightFromBytes(b []byte) (Height, error) {
	i, err := util.BytesToInt64(b)
	if err != nil {
		return NilHeight, errors.Wrap(err, "failed to NewHeightFromBytes")
	}

	return Height(i), nil
}

func (h Height) IsValid([]byte) error {
	if h < GenesisHeight {
		return util.InvalidError.Errorf("height must be greater than %d; height=%d", GenesisHeight, h)
	}

	return nil
}

// Int64 returns int64 of height.
func (h Height) Int64() int64 {
	return int64(h)
}

func (h Height) Bytes() []byte {
	return util.Int64ToBytes(int64(h))
}

func (h Height) String() string {
	return fmt.Sprintf("%d", h)
}

type Round uint64

func (r Round) Uint64() uint64 {
	return uint64(r)
}

func (r Round) Bytes() []byte {
	return util.Uint64ToBytes(uint64(r))
}

type Point struct {
	h Height
	r Round
}

func NewPoint(h Height, r Round) Point {
	return Point{h: h, r: r}
}

func (p Point) Height() Height {
	return p.h
}

func (p Point) Round() Round {
	return p.r
}

func (p Point) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(map[string]interface{}{
		"height": p.h,
		"round":  p.r,
	})
}

type pointJSONUnmarshaler struct {
	H Height `json:"height"`
	R Round  `json:"round"`
}

func (p *Point) UnmarshalJSON(b []byte) error {
	var u pointJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal point")
	}

	p.h = u.H
	p.r = u.R

	return nil
}
