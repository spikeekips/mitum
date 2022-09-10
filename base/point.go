package base

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
)

var (
	NilHeight      = Height(-1)
	GenesisHeight  = Height(0)
	GenesisPoint   = Point{h: GenesisHeight, r: Round(0)}
	ZeroStagePoint = StagePoint{Point: ZeroPoint, stage: StageUnknown}
	ZeroPoint      = Point{h: NilHeight, r: Round(0)}
)

var zeroPrefixHeightString = regexp.MustCompile(`^[0]+`)

// Height stands for height of Block
type Height int64

func NewHeightFromString(s string) (Height, error) {
	n := s
	if strings.HasPrefix(n, "0") {
		n = zeroPrefixHeightString.ReplaceAllString(n, "")

		if len(n) < 1 {
			n = "0"
		}
	}

	i, err := strconv.ParseInt(n, 10, 64)
	if err != nil {
		return NilHeight, errors.Wrap(err, "failed to NewHeightFromString")
	}

	return Height(i), nil
}

func NewHeightFromBytes(b []byte) (Height, error) {
	i, err := util.BigBytesToInt64(b)
	if err != nil {
		return NilHeight, errors.Wrap(err, "failed to NewHeightFromBytes")
	}

	return Height(i), nil
}

func (h Height) IsValid([]byte) error {
	if h < GenesisHeight {
		return util.ErrInvalid.Errorf("height must be greater than %d; height=%d", GenesisHeight, h)
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
	return util.Int64ToBigBytes(int64(h))
}

func (h Height) String() string {
	return fmt.Sprintf("%d", h)
}

func (h Height) Prev() Height {
	return h - 1
}

func (h Height) SafePrev() Height {
	if h <= GenesisHeight {
		return GenesisHeight
	}

	return h - 1
}

type Round uint64

func (r Round) Uint64() uint64 {
	return uint64(r)
}

func (r Round) Bytes() []byte {
	return util.Uint64ToBytes(uint64(r))
}

func (r Round) Prev() Round {
	if r <= 0 {
		return 0
	}

	return r - 1
}

type Point struct {
	util.DefaultJSONMarshaled
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

	if p.h == GenesisHeight && p.r != Round(0) {
		return errors.Errorf("invalid genesis point, %q", p)
	}

	return nil
}

func (p Point) Equal(b Point) bool {
	return p.h == b.h && p.r == b.r
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

// PrevRound returns previous round; if 0 round, returns previous height and zero
// round
func (p Point) PrevRound() Point {
	if p.Equal(GenesisPoint) {
		return GenesisPoint
	}

	var h Height
	var r Round

	switch {
	case p.r == 0:
		h = p.h.SafePrev()
		r = Round(0)
	default:
		h = p.h
		r = p.r.Prev()
	}

	return NewPoint(h, r)
}

func (p Point) NextRound() Point {
	return NewPoint(p.h, p.r+1)
}

// NextHeight returns next height with 0 round.
func (p Point) NextHeight() Point {
	return NewPoint(p.h+1, Round(0))
}

// PrevHeight returns previous height with 0 round
func (p Point) PrevHeight() Point {
	if p.h <= GenesisHeight {
		return p
	}

	return NewPoint(p.h-1, Round(0))
}

func (p Point) MarshalZerologObject(e *zerolog.Event) {
	e.Interface("height", p.h).Interface("round", p.r)
}

type pointJSONMarshaler struct {
	Height Height `json:"height"`
	Round  Round  `json:"round"`
}

func (p Point) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(pointJSONMarshaler{
		Height: p.h,
		Round:  p.r,
	})
}

type pointJSONUnmarshaler struct {
	Height HeightDecoder `json:"height"`
	Round  Round         `json:"round"`
}

func (p *Point) UnmarshalJSON(b []byte) error {
	var u pointJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal point")
	}

	p.h = u.Height.Height()
	p.r = u.Round

	return nil
}

type StagePoint struct {
	stage Stage
	util.DefaultJSONMarshaled
	Point
}

func NewStagePoint(point Point, stage Stage) StagePoint {
	return StagePoint{Point: point, stage: stage}
}

func (p StagePoint) Stage() Stage {
	return p.stage
}

func (p StagePoint) SetStage(s Stage) StagePoint {
	return NewStagePoint(p.Point, s)
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

func (p StagePoint) Equal(b StagePoint) bool {
	return p.stage == b.stage && p.Point.Equal(b.Point)
}

func (p StagePoint) Compare(b StagePoint) int {
	c := p.Point.Compare(b.Point)
	if c == 0 {
		return p.stage.Compare(b.stage)
	}

	return c
}

func (p StagePoint) Decrease() StagePoint {
	return NewStagePoint(p.Point.PrevHeight(), p.stage)
}

func (p StagePoint) MarshalZerologObject(e *zerolog.Event) {
	e.Interface("height", p.h).Interface("round", p.r).Stringer("stage", p.stage)
}

type stagePointJSONMarshaler struct {
	Stage Stage `json:"stage"`
	pointJSONMarshaler
}

func (p StagePoint) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(stagePointJSONMarshaler{
		pointJSONMarshaler: pointJSONMarshaler{
			p.h,
			p.r,
		},
		Stage: p.stage,
	})
}

type stagePointJSONUnmarshaler struct {
	Stage Stage `json:"stage"`
	pointJSONUnmarshaler
}

func (p *StagePoint) UnmarshalJSON(b []byte) error {
	var u stagePointJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal stage point")
	}

	p.h = u.Height.Height()
	p.r = u.Round
	p.stage = u.Stage

	return nil
}
