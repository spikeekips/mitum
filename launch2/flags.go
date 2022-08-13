package launch2

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
)

type BaseFlags struct {
	LoggingFlags `embed:"" prefix:"log."`
}

type AddressFlag struct {
	address base.Address
}

func (f *AddressFlag) UnmarshalText(b []byte) error {
	e := util.StringErrorFunc("failed to parse address flag")

	address, err := base.ParseStringAddress(string(b))
	if err != nil {
		return e(err, "")
	}

	if err := address.IsValid(nil); err != nil {
		return e(err, "")
	}

	f.address = address

	return nil
}

func (f AddressFlag) Address() base.Address {
	return f.address
}

type ConnInfoFlag struct {
	ci          quicstream.UDPConnInfo
	addr        string
	tlsinsecure bool
}

func (f *ConnInfoFlag) UnmarshalText(b []byte) error {
	e := util.StringErrorFunc("failed to parse ConnInfo flag")

	s := string(b)

	if err := network.IsValidAddr(s); err != nil {
		return e(err, "")
	}

	f.addr, f.tlsinsecure = network.ParseTLSInsecure(s)

	return nil
}

func (f ConnInfoFlag) String() string {
	return network.ConnInfoToString(f.addr, f.tlsinsecure)
}

func (f *ConnInfoFlag) ConnInfo() (quicstream.UDPConnInfo, error) {
	if f.ci.Addr() == nil {
		ci, err := quicstream.NewUDPConnInfoFromStringAddress(f.addr, f.tlsinsecure)
		if err != nil {
			return quicstream.UDPConnInfo{}, errors.Wrap(err, "failed to convert to quic ConnInfo")
		}

		f.ci = ci
	}

	return f.ci, nil
}

type HeightFlag struct {
	height *base.Height
}

func (f *HeightFlag) UnmarshalText(b []byte) error {
	e := util.StringErrorFunc("failed to parse address flag")

	if len(b) < 1 {
		f.height = &base.NilHeight

		return nil
	}

	height, err := base.NewHeightFromString(string(b))
	if err != nil {
		return e(err, "")
	}

	f.height = &height

	return nil
}

func (f *HeightFlag) IsSet() bool {
	if f.height == nil {
		return false
	}

	return *f.height >= base.NilHeight
}

func (f *HeightFlag) Height() base.Height {
	if f.height == nil {
		return base.NilHeight - 1
	}

	return *f.height
}
