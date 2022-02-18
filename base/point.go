package base

import (
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
)

var (
	NilHeight      = Height(-1)
	GenesisHeight  = Height(0)
	ZeroStagePoint = StagePoint{Point: ZeroPoint, stage: StageUnknown}
	ZeroPoint      = Point{h: NilHeight, r: Round(0)}
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

func (h Height) IsZero() bool {
	return h <= NilHeight
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

func RawPoint(h int64, r uint64) Point {
	return Point{h: Height(h), r: Round(r)}
}

func (p Point) Bytes() []byte {
	return util.ConcatByters(p.Height(), p.Round())
}

func (p Point) Height() Height {
	return p.h
}

func (p Point) Round() Round {
	return p.r
}

func (p Point) String() string {
	return fmt.Sprintf("{Point height=%d round=%d}", p.h, p.r)
}

func (p Point) IsValid([]byte) error {
	if err := p.h.IsValid(nil); err != nil {
		return errors.Wrapf(err, "invalid point")
	}

	return nil
}

func (p Point) Compare(b Point) int {
	switch {
	case p.Height() > b.Height():
		return 1
	case p.Height() < b.Height():
		return -1
	case p.Round() > b.Round():
		return 1
	case p.Round() < b.Round():
		return -1
	default:
		return 0
	}
}

func (p Point) IsZero() bool {
	return p.h.IsZero()
}

func (p Point) Prev() Point {
	switch {
	case p.r > 0:
		p.r--

		return p
	case p.h <= GenesisHeight:
		return p
	default:
		p.h--
		p.r = Round(0)

		return p
	}
}

func (p Point) Next() Point {
	p.h++
	p.r = Round(0)

	return p
}

func (p Point) NextRound() Point {
	p.r++

	return p
}

func (p Point) Decrease() Point {
	if p.h <= GenesisHeight {
		return p
	}

	return NewPoint(p.h-1, Round(0))
}

func (p Point) MarshalZerologObject(e *zerolog.Event) {
	e.Int64("height", p.h.Int64()).Uint64("round", p.r.Uint64())
}

type pointJSONMarshaler struct {
	H Height `json:"height"`
	R Round  `json:"round"`
}

func (p Point) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(pointJSONMarshaler{
		H: p.h,
		R: p.r,
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

type StagePoint struct {
	Point
	stage Stage
}

func NewStagePoint(point Point, stage Stage) StagePoint {
	return StagePoint{Point: point, stage: stage}
}

func (p StagePoint) Stage() Stage {
	return p.stage
}

func (p StagePoint) SetStage(s Stage) StagePoint {
	p.stage = s

	return p
}

func (p StagePoint) IsZero() bool {
	if p.Point.IsZero() {
		return true
	}

	err := p.stage.IsValid(nil)

	return err != nil
}

func (p StagePoint) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid stage point")
	if err := p.Point.IsValid(nil); err != nil {
		return e(err, "")
	}

	if err := p.stage.IsValid(nil); err != nil {
		return e(err, "")
	}

	return nil
}

func (p StagePoint) Bytes() []byte {
	return util.ConcatByters(p.Point, p.stage)
}

func (p StagePoint) String() string {
	return fmt.Sprintf("{StagePoint height=%d round=%d stage=%s}", p.h, p.r, p.stage)
}

func (p StagePoint) Compare(b StagePoint) int {
	c := p.Point.Compare(b.Point)
	if c == 0 {
		return p.stage.Compare(b.stage)
	}

	return c
}

func (p StagePoint) Decrease() StagePoint {
	p.Point = p.Point.Decrease()

	return p
}

func (p StagePoint) MarshalZerologObject(e *zerolog.Event) {
	e.Int64("height", p.h.Int64()).Uint64("round", p.r.Uint64()).Stringer("stage", p.stage)
}

type stagePointJSONMarshaler struct {
	pointJSONMarshaler
	S Stage `json:"stage"`
}

func (p StagePoint) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(stagePointJSONMarshaler{
		pointJSONMarshaler: pointJSONMarshaler{
			p.h,
			p.r,
		},
		S: p.stage,
	})
}

type stagePointJSONUnmarshaler struct {
	pointJSONUnmarshaler
	S Stage `json:"stage"`
}

func (p *StagePoint) UnmarshalJSON(b []byte) error {
	var u stagePointJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal stage point")
	}

	p.h = u.H
	p.r = u.R
	p.stage = u.S

	return nil
}
