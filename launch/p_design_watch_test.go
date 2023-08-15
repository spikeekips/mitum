package launch

import (
	"context"
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
)

type testReadDesign struct {
	suite.Suite
}

func (t *testReadDesign) TestLocalParams() {
	params := defaultLocalParams(util.UUID().Bytes())

	f := readLocalParams(params)

	t.Run("unknown key", func() {
		v, err := f("showme")
		t.Error(err)
		t.Nil(v)
		t.True(errors.Is(err, util.ErrNotFound))
	})

	t.Run("threshold", func() {
		v, err := f("isaac/threshold")
		t.NoError(err)
		t.NotNil(v)
		t.Equal(params.ISAAC.Threshold(), v)
	})

	t.Run("sync_source_checker_interval", func() {
		v, err := f("misc/sync_source_checker_interval")
		t.NoError(err)
		t.NotNil(v)
		t.Equal(util.ReadableDuration(params.MISC.SyncSourceCheckerInterval()), v)
	})

	t.Run("extra_same_member_limit", func() {
		v, err := f("memberlist/extra_same_member_limit")
		t.NoError(err)
		t.NotNil(v)
		t.Equal(params.Memberlist.ExtraSameMemberLimit(), v)
	})

	t.Run("network/ratelimit/suffrage", func() {
		v, err := f("network/ratelimit/suffrage")
		t.NoError(err)
		t.NotNil(v)
		t.Equal(params.Network.RateLimit().SuffrageRuleSet(), v)
	})
}

func (t *testReadDesign) TestHandler() {
	params := defaultLocalParams(util.UUID().Bytes())
	f := readDesign(params, nil, nil)

	encs := encoder.NewEncoders()
	enc := jsonenc.NewEncoder()
	t.NoError(encs.AddHinter(enc))

	t.NoError(enc.Add(encoder.DecodeDetail{Hint: quicstreamheader.DefaultResponseHeaderHint, Instance: quicstreamheader.DefaultResponseHeader{}}))
	t.NoError(enc.Add(encoder.DecodeDetail{Hint: ReadDesignHeaderHint, Instance: ReadDesignHeader{}}))

	priv := base.NewMPrivatekey()
	networkID := base.NetworkID(util.UUID().Bytes())
	ci := quicstream.UnsafeConnInfo(nil, true)

	t.Run("unknown key", func() {
		_, dialf := isaacnetwork.TestingDialFunc(encs, HandlerPrefixDesignRead, handlerDesignRead(priv.Publickey(), networkID, f))
		bdialf := quicstreamheader.NewDialFunc(dialf, encs, enc)

		stream, closef, err := bdialf(context.Background(), ci)
		t.NoError(err)
		defer closef()

		_, found, err := ReadDesignFromNetworkHandler(context.Background(), priv, networkID, "findme", stream)
		t.NoError(err)
		t.False(found)
	})

	t.Run("wrong priv", func() {
		wrongpub := base.NewMPrivatekey().Publickey()

		_, dialf := isaacnetwork.TestingDialFunc(encs, HandlerPrefixDesignRead, handlerDesignRead(wrongpub, networkID, f))
		bdialf := quicstreamheader.NewDialFunc(dialf, encs, enc)

		stream, closef, err := bdialf(context.Background(), ci)
		t.NoError(err)
		defer closef()

		_, found, err := ReadDesignFromNetworkHandler(context.Background(), priv, networkID, "findme", stream)
		t.Error(err)
		t.False(found)
		t.ErrorContains(err, "signature verification failed")
	})

	t.Run("threshold", func() {
		_, dialf := isaacnetwork.TestingDialFunc(encs, HandlerPrefixDesignRead, handlerDesignRead(priv.Publickey(), networkID, f))
		bdialf := quicstreamheader.NewDialFunc(dialf, encs, enc)

		stream, closef, err := bdialf(context.Background(), ci)
		t.NoError(err)
		defer closef()

		th, found, err := ReadDesignFromNetworkHandler(context.Background(), priv, networkID, "parameters/isaac/threshold", stream)
		t.NoError(err)
		t.True(found)
		t.Equal(params.ISAAC.Threshold().String(), th)
	})

	t.Run("threshold; interface{}", func() {
		_, dialf := isaacnetwork.TestingDialFunc(encs, HandlerPrefixDesignRead, handlerDesignRead(priv.Publickey(), networkID, f))
		bdialf := quicstreamheader.NewDialFunc(dialf, encs, enc)

		stream, closef, err := bdialf(context.Background(), ci)
		t.NoError(err)
		defer closef()

		th, found, err := ReadDesignFromNetworkHandler(context.Background(), priv, networkID, "parameters/isaac/threshold", stream)
		t.NoError(err)
		t.True(found)
		t.Equal(params.ISAAC.Threshold().String(), th)
	})
}

func TestReadDesign(t *testing.T) {
	suite.Run(t, new(testReadDesign))
}

type testWriteDesign struct {
	suite.Suite
	log *logging.Logging
}

func (t *testWriteDesign) SetupSuite() {
	t.log = logging.TestNilLogging
}

func (t *testWriteDesign) TestNew() {
	prefix := util.UUID().String()

	var currentValue string

	wf := writeDesignKey(func(key, nextkey, value string) (interface{}, interface{}, error) {
		switch key {
		case "showme":
			prev := currentValue
			currentValue = value

			return prev, currentValue, nil
		default:
			return nil, nil, util.ErrNotFound.Errorf("key not found, %q", key)
		}
	})

	f := writeDesignValueFromConsul(prefix, wf, t.log)

	var createIndex uint64
	t.Run("short key", func() {
		updated, err := f("a", "", createIndex)
		t.False(updated)
		t.Error(err)
		t.ErrorContains(err, "wrong key")
	})

	t.Run("unknown key", func() {
		updated, err := f(prefix+"findme", "", createIndex)
		t.False(updated)
		t.Error(err)
		t.True(errors.Is(err, util.ErrNotFound))
	})

	t.Run("ok", func() {
		value := util.UUID().String()

		updated, err := f(prefix+"showme", value, createIndex)
		t.NoError(err)
		t.True(updated)

		t.Equal(value, currentValue)
	})

	t.Run("old", func() {
		updated, err := f(prefix+"showme", "", createIndex) // createIndex not updated
		t.False(updated)
		t.NoError(err)
	})

	t.Run("update again", func() {
		createIndex++

		value := util.UUID().String()

		updated, err := f(prefix+"showme", value, createIndex)
		t.NoError(err)
		t.True(updated)

		t.Equal(value, currentValue)
	})
}

func (t *testWriteDesign) TestSet() {
	prefix := util.UUID().String()

	var currentValue string

	wf := writeDesignKey(func(key, nextkey, value string) (interface{}, interface{}, error) {
		switch key {
		case "root":
			return writeDesignKey(func(key, nextkey, value string) (prev interface{}, next interface{}, err error) {
				t.T().Logf("> root: key=%q prefix=%q nextkey=%q value=%q", key, prefix, nextkey, value)

				switch key {
				case "isaac":
					value += "-isaac"
				case "misc":
					value += "-misc"
				default:
					return prev, next, errors.Errorf("unknown key, %q", key)
				}

				prev = currentValue
				currentValue = value

				t.T().Logf("< root: prev=%q current=%q", prev, currentValue)

				return prev, currentValue, nil
			})(nextkey, value)
		case "child":
			return writeDesignKey(func(key, nextkey, value string) (prev interface{}, next interface{}, err error) {
				t.T().Logf("> child: key=%q prefix=%q nextkey=%q value=%q", key, prefix, nextkey, value)

				switch key {
				case "isaac":
					value += "-child/isaac"
				case "misc":
					value += "-child/misc"

					return writeDesignKey(func(key, nextkey, value string) (prev interface{}, next interface{}, err error) {
						t.T().Logf("> child/misc: key=%q prefix=%q nextkey=%q value=%q", key, prefix, nextkey, value)

						switch key {
						case "a":
							value += "-a"
						case "b":
							value += "-b"
						default:
							return prev, next, errors.Errorf("unknown key, %q", key)
						}

						prev = currentValue
						currentValue = value

						t.T().Logf("< child/misc: prev=%q current=%q", prev, currentValue)

						return prev, currentValue, nil
					})(nextkey, value)
				default:
					return prev, next, errors.Errorf("unknown key, %q", key)
				}

				prev = currentValue
				currentValue = value

				t.T().Logf("< child: prev=%q current=%q", prev, currentValue)

				return prev, currentValue, nil
			})(nextkey, value)
		default:
			return nil, nil, util.ErrNotFound.Errorf("key not found, %q", key)
		}
	})

	f := writeDesignValueFromConsul(prefix, wf, t.log)

	var createIndex uint64

	t.Run("ok", func() {
		createIndex++

		value := util.UUID().String()

		updated, err := f(prefix+"root/isaac", value, createIndex)
		t.NoError(err)
		t.True(updated)

		t.Equal(value+"-isaac", currentValue)
	})

	t.Run("end slash", func() {
		createIndex++

		value := util.UUID().String()

		updated, err := f(prefix+"root/misc/", value, createIndex)
		t.NoError(err)
		t.True(updated)

		t.Equal(value+"-misc", currentValue)
	})

	t.Run("child/misc/a", func() {
		createIndex++

		value := util.UUID().String()

		updated, err := f(prefix+"child/misc/a", value, createIndex)
		t.NoError(err)
		t.True(updated)

		t.Equal(value+"-child/misc-a", currentValue)
	})
}

func (t *testWriteDesign) TestHandler() {
	encs := encoder.NewEncoders()
	enc := jsonenc.NewEncoder()
	t.NoError(encs.AddHinter(enc))

	t.NoError(enc.Add(encoder.DecodeDetail{Hint: quicstreamheader.DefaultResponseHeaderHint, Instance: quicstreamheader.DefaultResponseHeader{}}))
	t.NoError(enc.Add(encoder.DecodeDetail{Hint: ReadDesignHeaderHint, Instance: ReadDesignHeader{}}))
	t.NoError(enc.Add(encoder.DecodeDetail{Hint: WriteDesignHeaderHint, Instance: WriteDesignHeader{}}))

	priv := base.NewMPrivatekey()
	networkID := base.NetworkID(util.UUID().Bytes())
	ci := quicstream.UnsafeConnInfo(nil, true)

	params := defaultLocalParams(util.UUID().Bytes())
	wf := writeDesign(enc, NodeDesign{}, params, nil, nil)

	t.Run("unknown key", func() {
		_, dialf := isaacnetwork.TestingDialFunc(encs, HandlerPrefixDesignWrite, handlerDesignWrite(priv.Publickey(), networkID, wf))
		bdialf := quicstreamheader.NewDialFunc(dialf, encs, enc)

		stream, closef, err := bdialf(context.Background(), ci)
		t.NoError(err)
		defer closef()

		found, err := WriteDesignFromNetworkHandler(context.Background(), priv, networkID, "findme", "33.0", stream)
		t.NoError(err)
		t.False(found)
	})

	t.Run("wrong priv", func() {
		wrongpub := base.NewMPrivatekey().Publickey()

		_, dialf := isaacnetwork.TestingDialFunc(encs, HandlerPrefixDesignWrite, handlerDesignWrite(wrongpub, networkID, wf))
		bdialf := quicstreamheader.NewDialFunc(dialf, encs, enc)

		stream, closef, err := bdialf(context.Background(), ci)
		t.NoError(err)
		defer closef()

		found, err := WriteDesignFromNetworkHandler(context.Background(), priv, networkID, "findme", "33.0", stream)
		t.Error(err)
		t.False(found)
		t.ErrorContains(err, "signature verification failed")
	})

	t.Run("wrong threshold", func() {
		_, dialf := isaacnetwork.TestingDialFunc(encs, HandlerPrefixDesignWrite, handlerDesignWrite(priv.Publickey(), networkID, wf))
		bdialf := quicstreamheader.NewDialFunc(dialf, encs, enc)

		stream, closef, err := bdialf(context.Background(), ci)
		t.NoError(err)
		defer closef()

		_, err = WriteDesignFromNetworkHandler(context.Background(), priv, networkID, "parameters/isaac/threshold", "a33.0", stream)
		t.Error(err)
		t.ErrorContains(err, "invalid threshold")
	})

	t.Run("threshold", func() {
		_, dialf := isaacnetwork.TestingDialFunc(encs, HandlerPrefixDesignWrite, handlerDesignWrite(priv.Publickey(), networkID, wf))
		bdialf := quicstreamheader.NewDialFunc(dialf, encs, enc)

		stream, closef, err := bdialf(context.Background(), ci)
		t.NoError(err)
		defer closef()

		found, err := WriteDesignFromNetworkHandler(context.Background(), priv, networkID, "parameters/isaac/threshold", "33.0", stream)
		t.NoError(err)
		t.True(found)
		t.Equal(base.Threshold(33.0), params.ISAAC.Threshold())
	})

	t.Run("interval_broadcast_ballot", func() {
		_, dialf := isaacnetwork.TestingDialFunc(encs, HandlerPrefixDesignWrite, handlerDesignWrite(priv.Publickey(), networkID, wf))
		bdialf := quicstreamheader.NewDialFunc(dialf, encs, enc)

		stream, closef, err := bdialf(context.Background(), ci)
		t.NoError(err)
		defer closef()

		prev := params.ISAAC.IntervalBroadcastBallot()
		newvalue := prev + 99

		found, err := WriteDesignFromNetworkHandler(context.Background(), priv, networkID, "parameters/isaac/interval_broadcast_ballot", fmt.Sprintf("%q", newvalue.String()), stream)
		t.NoError(err)
		t.True(found)
		t.Equal(newvalue, params.ISAAC.IntervalBroadcastBallot())
	})
}

func TestWriteDesign(t *testing.T) {
	suite.Run(t, new(testWriteDesign))
}
