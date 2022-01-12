package base

import (
	"fmt"
	"strconv"

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
		return NilHeight, err
	}

	return Height(i), nil
}

func NewHeightFromBytes(b []byte) (Height, error) {
	i, err := util.BytesToInt64(b)
	if err != nil {
		return NilHeight, err
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
