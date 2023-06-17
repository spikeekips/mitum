package quicmemberlist

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
)

type testMemberlist struct {
	quicstream.BaseTest
	encs *encoder.Encoders
	enc  *jsonenc.Encoder
}

func (t *testMemberlist) SetupSuite() {
	t.BaseTest.SetupSuite()

	t.encs = encoder.NewEncoders()
	t.enc = jsonenc.NewEncoder()
	t.NoError(t.encs.AddHinter(t.enc))

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: MemberHint, Instance: BaseMember{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ConnInfoBroadcastMessageHint, Instance: ConnInfoBroadcastMessage{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: CallbackBroadcastMessageHeaderHint, Instance: CallbackBroadcastMessageHeader{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: EnsureBroadcastMessageHeaderHint, Instance: EnsureBroadcastMessageHeader{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: quicstreamheader.DefaultResponseHeaderHint, Instance: quicstreamheader.DefaultResponseHeader{}}))
}

func (t *testMemberlist) SetupTest() {
	t.BaseTest.SetupTest()
}

func (t *testMemberlist) newConnInfo() quicstream.ConnInfo {
	addr := t.BaseTest.NewAddr()

	return quicstream.NewConnInfo(addr, true)
}

func (t *testMemberlist) newargs(config *memberlist.Config) *MemberlistArgs {
	return NewMemberlistArgs(t.enc, config)
}

func (t *testMemberlist) TestNew() {
	bind := t.NewAddr()
	config := DefaultMemberlistConfig(bind.String(), bind, bind)

	local, err := NewMember(bind.String(), bind, base.RandomAddress(""), base.NewMPrivatekey().Publickey(), "1.2.3.4:4321", true)
	t.NoError(err)

	config.Delegate = NewDelegate(local, nil, nil)
	config.Transport = &Transport{args: NewTransportArgs()}
	config.Alive = NewAliveDelegate(t.enc, local.Addr(), nil, nil)

	args := t.newargs(config)

	srv, err := NewMemberlist(local, args)
	t.NoError(err)

	t.NoError(srv.Start(context.Background()))
	defer t.NoError(srv.Stop())
}

func (t *testMemberlist) newServersForJoining(
	node base.Address,
	ci quicstream.ConnInfo,
	whenJoined DelegateJoinedFunc,
	whenLeft DelegateLeftFunc,
) (
	*quicstream.PrefixHandler,
	*Memberlist,
	func() error,
	func(),
) {
	transportprefix := quicstream.HashPrefix("tp")
	tlsconfig := t.NewTLSConfig(t.Proto)

	poolclient := quicstream.NewPoolClient()

	laddr := ci.UDPAddr()
	t.T().Log("addr:", laddr)

	transport := NewTransportWithQuicstream(
		laddr,
		transportprefix,
		poolclient,
		func(ci quicstream.ConnInfo) func(*net.UDPAddr) *quicstream.Client {
			return func(*net.UDPAddr) *quicstream.Client {
				return quicstream.NewClient(
					ci.UDPAddr(),
					&tls.Config{
						InsecureSkipVerify: ci.TLSInsecure(),
						NextProtos:         []string{t.Proto},
					},
					nil,
					nil,
				)
			}
		},
		func(string) bool { return false },
		nil,
	)

	ph := quicstream.NewPrefixHandler(nil)
	ph.Add(transportprefix, transport.QuicstreamHandler)

	quicstreamsrv, err := quicstream.NewServer(laddr, tlsconfig, nil, ph.Handler)
	t.NoError(err)
	quicstreamsrv.SetLogging(logging.TestNilLogging)

	local, err := NewMember(laddr.String(), laddr, node, base.NewMPrivatekey().Publickey(), laddr.String(), true)
	t.NoError(err)

	memberlistconfig := DefaultMemberlistConfig(local.Name(), laddr, laddr)
	memberlistconfig.Transport = transport
	memberlistconfig.Events = NewEventsDelegate(t.enc, whenJoined, whenLeft)
	memberlistconfig.Alive = NewAliveDelegate(t.enc, laddr, func(Member) error { return nil }, func(Member) error { return nil })

	memberlistconfig.Delegate = NewDelegate(local, nil, nil)

	args := t.newargs(memberlistconfig)

	srv, _ := NewMemberlist(local, args)
	srv.SetLogging(logging.TestNilLogging)

	return ph, srv,
		func() error {
			if err := quicstreamsrv.Start(context.Background()); err != nil {
				return err
			}

			if err := srv.Start(context.Background()); err != nil {
				return err
			}

			return nil
		},
		func() {
			quicstreamsrv.Stop()
			srv.Stop()
		}
}

func (t *testMemberlist) TestLocalJoinAlone() {
	lci := t.newConnInfo()
	lnode := base.RandomAddress("")

	joinedch := make(chan Member, 1)
	_, srv, startf, stopf := t.newServersForJoining(
		lnode,
		lci,
		func(node Member) {
			joinedch <- node
		},
		nil,
	)

	t.NoError(startf())
	defer stopf()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("local failed to join"))
	case node := <-joinedch:
		t.True(isEqualAddress(lci, node))
	}

	t.Equal(1, srv.MembersLen())

	var joined []Member
	srv.Members(func(node Member) bool {
		joined = append(joined, node)

		return true
	})
	t.Equal(1, len(joined))
	t.True(isEqualAddress(lci, joined[0]))
}

func (t *testMemberlist) TestLocalJoinAloneAndRejoin() {
	lci := t.newConnInfo()
	lnode := base.RandomAddress("")

	joinedch := make(chan Member, 1)
	leftch := make(chan Member, 1)
	_, srv, startf, stopf := t.newServersForJoining(
		lnode,
		lci,
		func(node Member) {
			joinedch <- node
		},
		func(node Member) {
			leftch <- node
		},
	)

	t.NoError(startf())
	defer stopf()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("local failed to join"))
	case node := <-joinedch:
		t.True(isEqualAddress(lci, node))
	}

	t.Equal(1, srv.MembersLen())

	var joined []Member
	srv.Members(func(node Member) bool {
		joined = append(joined, node)

		return true
	})
	t.Equal(1, len(joined))
	t.True(isEqualAddress(lci, joined[0]))

	t.Run("leave", func() {
		t.NoError(srv.Leave(time.Second * 10))

		select {
		case <-time.After(time.Second * 10):
			t.NoError(errors.Errorf("local failed to left"))
		case node := <-leftch:
			t.True(isEqualAddress(lci, node))
			t.Equal(0, srv.MembersLen())
		}
	})

	t.Run("join again", func() {
		t.NoError(srv.Join([]quicstream.ConnInfo{lci}))

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("local failed to left"))
		case node := <-joinedch:
			t.True(isEqualAddress(lci, node))
			t.Equal(1, srv.MembersLen())
		}
	})
}

func (t *testMemberlist) TestLocalJoinToRemote() {
	lci := t.newConnInfo()
	lnode := base.RandomAddress("")
	rci := t.newConnInfo()
	rnode := base.RandomAddress("")

	addrs := []*net.UDPAddr{lci.UDPAddr(), rci.UDPAddr()}
	sort.Slice(addrs, func(i, j int) bool {
		return strings.Compare(addrs[i].String(), addrs[j].String()) < 0
	})

	ljoinedch := make(chan Member, 1)
	rjoinedch := make(chan Member, 1)
	_, lsrv, lstartf, lstopf := t.newServersForJoining(
		lnode,
		lci,
		func(node Member) {
			if isEqualAddress(node, lci) {
				return
			}

			ljoinedch <- node
		},
		nil,
	)

	_, rsrv, rstartf, rstopf := t.newServersForJoining(
		rnode,
		rci,
		func(node Member) {
			if isEqualAddress(node, rci) {
				return
			}

			rjoinedch <- node
		},
		nil,
	)

	t.NoError(lstartf())
	defer lstopf()
	t.NoError(rstartf())
	defer rstopf()

	<-time.After(time.Second)
	t.NoError(lsrv.Join([]quicstream.ConnInfo{rci}))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("local failed to join to remote"))
	case node := <-ljoinedch:
		t.True(isEqualAddress(rci, node))

		t.Equal(2, lsrv.MembersLen())

		var joined []Member
		lsrv.Members(func(node Member) bool {
			joined = append(joined, node)

			return true
		})
		t.Equal(2, len(joined))

		sort.Slice(joined, func(i, j int) bool {
			return strings.Compare(joined[i].Addr().String(), joined[j].Addr().String()) < 0
		})
		t.True(isEqualAddress(addrs[0], joined[0]))
		t.True(isEqualAddress(addrs[1], joined[1]))
	}

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("remote failed to join to local"))
	case node := <-rjoinedch:
		t.True(isEqualAddress(lci, node))

		t.Equal(2, rsrv.MembersLen())

		var joined []Member
		rsrv.Members(func(node Member) bool {
			joined = append(joined, node)

			return true
		})
		t.Equal(2, len(joined))

		sort.Slice(joined, func(i, j int) bool {
			return strings.Compare(joined[i].Addr().String(), joined[j].Addr().String()) < 0
		})
		t.True(isEqualAddress(addrs[0], joined[0]))
		t.True(isEqualAddress(addrs[1], joined[1]))
	}
}

func (t *testMemberlist) TestLocalJoinToRemoteButFailedToChallenge() {
	lci := t.newConnInfo()
	lnode := base.RandomAddress("")
	rci := t.newConnInfo()
	rnode := base.RandomAddress("")

	rjoinedch := make(chan Member, 1)
	_, lsrv, lstartf, lstopf := t.newServersForJoining(
		lnode,
		lci,
		nil,
		nil,
	)

	lsrv.args.PatchedConfig.Alive = NewAliveDelegate(
		t.enc,
		lci.UDPAddr(),
		func(node Member) error {
			if isEqualAddress(node, rci) {
				return errors.Errorf("failed to challenge")
			}

			return nil
		},
		func(node Member) error { return nil },
	)

	args := t.newargs(lsrv.args.PatchedConfig)
	lsrv, _ = NewMemberlist(lsrv.local, args)

	_, rsrv, rstartf, rstopf := t.newServersForJoining(
		rnode,
		rci,
		func(node Member) {
			if isEqualAddress(node, rci) {
				return
			}

			rjoinedch <- node
		},
		nil,
	)

	t.NoError(lstartf())
	defer lstopf()
	t.NoError(rstartf())
	defer rstopf()

	<-time.After(time.Second)
	err := lsrv.Join([]quicstream.ConnInfo{rci})
	t.NoError(err)

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("local failed to join"))
	case node := <-rjoinedch:
		t.True(isEqualAddress(lci, node))
	}

	t.Equal(1, lsrv.MembersLen())
	t.Equal(2, rsrv.MembersLen())
}

func (t *testMemberlist) TestLocalJoinToRemoteButNotAllowed() {
	lci := t.newConnInfo()
	lnode := base.RandomAddress("")
	rci := t.newConnInfo()
	rnode := base.RandomAddress("")

	rjoinedch := make(chan Member, 1)
	_, lsrv, lstartf, lstopf := t.newServersForJoining(
		lnode,
		lci,
		nil,
		nil,
	)

	lsrv.args.PatchedConfig.Alive = NewAliveDelegate(
		t.enc,
		lci.UDPAddr(),
		func(node Member) error { return nil },
		func(node Member) error {
			if isEqualAddress(node, rci) {
				return errors.Errorf("remote disallowed")
			}

			return nil
		},
	)

	args := t.newargs(lsrv.args.PatchedConfig)
	lsrv, _ = NewMemberlist(lsrv.local, args)

	_, rsrv, rstartf, rstopf := t.newServersForJoining(
		rnode,
		rci,
		func(node Member) {
			if isEqualAddress(node, rci) {
				return
			}

			rjoinedch <- node
		},
		nil,
	)

	t.NoError(lstartf())
	defer lstopf()
	t.NoError(rstartf())
	defer rstopf()

	<-time.After(time.Second)
	err := lsrv.Join([]quicstream.ConnInfo{rci})
	t.NoError(err)

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("local failed to join"))
	case node := <-rjoinedch:
		t.True(isEqualAddress(lci, node))
	}

	t.Equal(1, lsrv.MembersLen())
	t.Equal(2, rsrv.MembersLen())
}

func (t *testMemberlist) TestLocalLeave() {
	lci := t.newConnInfo()
	lnode := base.RandomAddress("")
	rci := t.newConnInfo()
	rnode := base.RandomAddress("")

	ljoinedch := make(chan Member, 3)
	rjoinedch := make(chan Member, 3)

	lleftch := make(chan Member, 3)
	rleftch := make(chan Member, 3)
	_, lsrv, lstartf, lstopf := t.newServersForJoining(
		lnode,
		lci,
		func(node Member) {
			if isEqualAddress(node, lci) {
				return
			}

			ljoinedch <- node
		},
		func(node Member) {
			lleftch <- node
		},
	)

	_, rsrv, rstartf, rstopf := t.newServersForJoining(
		rnode,
		rci,
		func(node Member) {
			if isEqualAddress(node, rci) {
				return
			}

			rjoinedch <- node
		},
		func(node Member) {
			rleftch <- node
		},
	)

	t.NoError(lstartf())
	defer lstopf()
	t.NoError(rstartf())
	defer rstopf()

	<-time.After(time.Second)
	t.NoError(lsrv.Join([]quicstream.ConnInfo{rci}))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("local failed to join to remote"))
	case node := <-ljoinedch:
		t.True(isEqualAddress(rci, node))
	}

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("remote failed to join to local"))
	case node := <-rjoinedch:
		t.True(isEqualAddress(lci, node))
	}

	t.T().Log("all nodes joined; local leaves")

	t.NoError(lsrv.Leave(time.Second * 10))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("local failed to leave from local"))
	case node := <-lleftch:
		t.True(isEqualAddress(lci, node))
		t.Equal(0, lsrv.MembersLen())
	}

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("local failed to leave from remote"))
	case node := <-rleftch:
		t.True(isEqualAddress(lci, node))
		t.Equal(1, rsrv.MembersLen())
	}

	t.Run("join again", func() {
		t.NoError(lsrv.Join([]quicstream.ConnInfo{rci}))

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("local failed to join to remote"))
		case node := <-ljoinedch:
			t.True(isEqualAddress(rci, node))
		}

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("remote failed to join to local"))
		case node := <-rjoinedch:
			t.True(isEqualAddress(lci, node))
		}
	})
}

func (t *testMemberlist) TestLocalShutdownAndLeave() {
	lci := t.newConnInfo()
	lnode := base.RandomAddress("")
	rci := t.newConnInfo()
	rnode := base.RandomAddress("")

	ljoinedch := make(chan Member, 1)
	rjoinedch := make(chan Member, 1)

	rleftch := make(chan Member, 1)
	_, lsrv, lstartf, lstopf := t.newServersForJoining(
		lnode,
		lci,
		func(node Member) {
			if isEqualAddress(node, lci) {
				return
			}

			ljoinedch <- node
		},
		nil,
	)

	_, rsrv, rstartf, rstopf := t.newServersForJoining(
		rnode,
		rci,
		func(node Member) {
			if isEqualAddress(node, rci) {
				return
			}

			rjoinedch <- node
		},
		func(node Member) {
			rleftch <- node
		},
	)

	t.NoError(lstartf())
	defer lstopf()
	t.NoError(rstartf())
	defer rstopf()

	<-time.After(time.Second)
	t.NoError(lsrv.Join([]quicstream.ConnInfo{rci}))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("local failed to join to remote"))
	case node := <-ljoinedch:
		t.True(isEqualAddress(rci, node))
	}

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("remote failed to join to local"))
	case node := <-rjoinedch:
		t.True(isEqualAddress(lci, node))
	}

	t.T().Log("all nodes joined; local shutdown")

	t.NoError(lsrv.Stop())

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("local failed to leave from remote"))
	case node := <-rleftch:
		t.True(isEqualAddress(lci, node))
		t.Equal(1, rsrv.MembersLen())
	}
}

func (t *testMemberlist) TestJoinMultipleNodeWithSameName() {
	lci := t.newConnInfo()
	lnode := base.RandomAddress("")

	rnode := base.RandomAddress("")
	rci0 := t.newConnInfo()
	rci1 := t.newConnInfo()

	ljoinedch := make(chan Member, 1)
	_, lsrv, lstartf, lstopf := t.newServersForJoining(
		lnode,
		lci,
		func(node Member) {
			if isEqualAddress(node, lci) {
				return
			}

			ljoinedch <- node
		},
		nil,
	)

	lsrv.args.ExtraSameMemberLimit = func() uint64 { return 3 }

	_, _, rstartf0, rstopf0 := t.newServersForJoining(rnode, rci0, nil, nil)
	_, _, rstartf1, rstopf1 := t.newServersForJoining(rnode, rci1, nil, nil)

	t.NoError(lstartf())
	defer lstopf()
	t.NoError(rstartf0())
	defer rstopf0()
	t.NoError(rstartf1())
	defer rstopf1()

	alljoinedch := make(chan error, 1)

	go func() {
		after := time.After(time.Second * 3)

		var r0, r1 bool
		for {
			select {
			case <-after:
				alljoinedch <- errors.Errorf("local failed to join to remote")

				return
			case node := <-ljoinedch:
				switch {
				case isEqualAddress(rci0, node):
					r0 = true
				case isEqualAddress(rci1, node):
					r1 = true
				}

				if r0 && r1 {
					alljoinedch <- nil

					return
				}
			}
		}
	}()

	<-time.After(time.Second)
	t.T().Logf("trying to join to remotes, %q, %q", rci0, rci1)
	t.NoError(lsrv.Join([]quicstream.ConnInfo{rci0, rci1}))

	err := <-alljoinedch
	t.NoError(err)

	t.Equal(3, lsrv.MembersLen())
	t.Equal(2, lsrv.members.MembersLen(rnode))
}

func (t *testMemberlist) TestLocalOverMemberLimit() {
	lci := t.newConnInfo()
	lnode := base.SimpleAddress("local")

	rci0 := t.newConnInfo()
	rnode := base.SimpleAddress("remote")

	ljoinedch := make(chan Member, 1)
	_, lsrv, lstartf, lstopf := t.newServersForJoining(
		lnode,
		lci,
		func(node Member) {
			if isEqualAddress(node, lci) {
				return
			}

			ljoinedch <- node
		},
		nil,
	)

	lsrv.args.ExtraSameMemberLimit = func() uint64 { return 0 } // NOTE only allow 1 member in node name

	_, _, rstartf0, rstopf0 := t.newServersForJoining(
		rnode,
		rci0,
		nil,
		nil,
	)

	t.NoError(lstartf())
	defer lstopf()
	t.NoError(rstartf0())
	defer rstopf0()

	<-time.After(time.Second)
	t.NoError(lsrv.Join([]quicstream.ConnInfo{rci0}))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("local failed to join to remote"))
	case node := <-ljoinedch:
		t.True(isEqualAddress(rci0, node))

		t.Equal(2, lsrv.MembersLen())
		t.Equal(1, lsrv.members.MembersLen(rnode))
	}

	t.T().Log("new remote node trying to join to local")

	rci1 := t.newConnInfo()

	_, rsrv1, rstartf1, rstopf1 := t.newServersForJoining(
		rnode,
		rci1,
		nil,
		nil,
	)

	t.NoError(rstartf1())
	defer rstopf1()

	<-time.After(time.Second)
	t.NoError(rsrv1.Join([]quicstream.ConnInfo{lci}))

	<-time.After(time.Second * 3)
	t.Equal(2, lsrv.MembersLen())

	n, others, found := lsrv.members.MembersLenOthers(rnode, rci0.UDPAddr())
	t.True(found)
	t.Equal(1, n)
	t.Equal(0, others)

	var joinedremotes []Member
	lsrv.Members(func(node Member) bool {
		if node.Address().Equal(rnode) {
			joinedremotes = append(joinedremotes, node)
		}

		return true
	})

	t.Equal(2, lsrv.MembersLen())
	t.Equal(1, len(joinedremotes))
	t.True(isEqualAddress(rci0, joinedremotes[0].Addr()))
}

func (t *testMemberlist) TestLocalJoinToRemoteWithInvalidNode() {
	lci := t.newConnInfo()
	lnode := base.RandomAddress("")
	rci := t.newConnInfo()
	rnode := base.RandomAddress("")

	addrs := []*net.UDPAddr{lci.UDPAddr(), rci.UDPAddr()}
	sort.Slice(addrs, func(i, j int) bool {
		return strings.Compare(addrs[i].String(), addrs[j].String()) < 0
	})

	ljoinedch := make(chan Member, 1)
	rjoinedch := make(chan Member, 1)
	_, lsrv, lstartf, lstopf := t.newServersForJoining(
		lnode,
		lci,
		func(node Member) {
			if isEqualAddress(node, lci) {
				return
			}

			ljoinedch <- node
		},
		nil,
	)

	_, rsrv, rstartf, rstopf := t.newServersForJoining(
		rnode,
		rci,
		func(node Member) {
			if isEqualAddress(node, rci) {
				return
			}

			rjoinedch <- node
		},
		nil,
	)

	remote, err := NewMember(rci.UDPAddr().String(), rci.UDPAddr(), rnode, base.NewMPrivatekey().Publickey(), "", true) // NOTE empty publish
	t.NoError(err)
	err = remote.IsValid(nil)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "empty publish")

	rdelegate := rsrv.args.PatchedConfig.Delegate.(*Delegate)
	rdelegate.local = remote

	t.NoError(lstartf())
	defer lstopf()
	t.NoError(rstartf())
	defer rstopf()

	<-time.After(time.Second)
	err = lsrv.Join([]quicstream.ConnInfo{rci})
	t.NoError(err)

	select {
	case <-time.After(time.Second * 2):
	case <-ljoinedch:
		t.NoError(errors.Errorf("unexpected; local joined to remote"))
	}
}

func (t *testMemberlist) TestJoinWithDeadNode() {
	lci := t.newConnInfo()
	lnode := base.RandomAddress("")
	rci := t.newConnInfo()

	addrs := []*net.UDPAddr{lci.UDPAddr(), rci.UDPAddr()}
	sort.Slice(addrs, func(i, j int) bool {
		return strings.Compare(addrs[i].String(), addrs[j].String()) < 0
	})

	_, lsrv, lstartf, lstopf := t.newServersForJoining(
		lnode,
		lci,
		func(node Member) {},
		nil,
	)

	t.NoError(lstartf())
	defer lstopf()

	err := lsrv.Join([]quicstream.ConnInfo{rci})
	t.Error(err)
}

func (t *testMemberlist) checkJoined(
	lsrv, rsrv *Memberlist,
	ljoinedch, rjoinedch chan Member,
) {
	lci := lsrv.local.ConnInfo()
	rci := rsrv.local.ConnInfo()

	addrs := []*net.UDPAddr{lsrv.local.Addr(), rsrv.local.Addr()}
	sort.Slice(addrs, func(i, j int) bool {
		return strings.Compare(addrs[i].String(), addrs[j].String()) < 0
	})

	t.NoError(lsrv.Join([]quicstream.ConnInfo{rci}))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("local failed to join to remote"))
	case node := <-ljoinedch:
		t.True(isEqualAddress(rci, node))

		t.Equal(2, lsrv.MembersLen())

		var joined []Member
		lsrv.Members(func(node Member) bool {
			joined = append(joined, node)

			return true
		})
		t.Equal(2, len(joined))

		sort.Slice(joined, func(i, j int) bool {
			return strings.Compare(joined[i].Addr().String(), joined[j].Addr().String()) < 0
		})
		t.True(isEqualAddress(addrs[0], joined[0]))
		t.True(isEqualAddress(addrs[1], joined[1]))
	}

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("remote failed to join to local"))
	case node := <-rjoinedch:
		t.True(isEqualAddress(lci, node))

		t.Equal(2, rsrv.MembersLen())

		var joined []Member
		rsrv.Members(func(node Member) bool {
			joined = append(joined, node)

			return true
		})
		t.Equal(2, len(joined))

		sort.Slice(joined, func(i, j int) bool {
			return strings.Compare(joined[i].Addr().String(), joined[j].Addr().String()) < 0
		})
		t.True(isEqualAddress(addrs[0], joined[0]))
		t.True(isEqualAddress(addrs[1], joined[1]))
	}
}

func (t *testMemberlist) checkJoined2(
	lsrv *Memberlist,
	ljoinedch chan struct{},
	rsrvs []*Memberlist,
	rjoinedchs []chan struct{},
) {
	rcis := make([]quicstream.ConnInfo, len(rsrvs))

	expectedjoined := make([]string, len(rsrvs)+1)
	expectedjoined[0] = lsrv.local.ConnInfo().String()
	for i := range rsrvs {
		rcis[i] = rsrvs[i].local.ConnInfo()
		expectedjoined[i+1] = rsrvs[i].local.ConnInfo().String()
	}

	sort.Slice(expectedjoined, func(i, j int) bool {
		return strings.Compare(expectedjoined[i], expectedjoined[j]) < 0
	})

	t.T().Log("rcis:", rcis)
	t.NoError(lsrv.Join(rcis))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("local failed to join to remote"))
	case <-ljoinedch:
		t.Equal(len(rsrvs)+1, lsrv.MembersLen())

		var joined []string
		lsrv.Members(func(node Member) bool {
			joined = append(joined, node.ConnInfo().String())

			return true
		})
		t.Equal(len(rsrvs)+1, len(joined))

		sort.Slice(joined, func(i, j int) bool {
			return strings.Compare(joined[i], joined[j]) < 0
		})
		t.Equal(expectedjoined, joined)
	}

	for i := range rjoinedchs {
		srv := rsrvs[i]
		ch := rjoinedchs[i]

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("remote failed to join"))
		case <-ch:
			t.Equal(len(rsrvs)+1, srv.MembersLen())

			var joined []string
			srv.Members(func(node Member) bool {
				joined = append(joined, node.ConnInfo().String())

				return true
			})
			t.Equal(len(rsrvs)+1, len(joined))

			sort.Slice(joined, func(i, j int) bool {
				return strings.Compare(joined[i], joined[j]) < 0
			})
			t.Equal(expectedjoined, joined)
		}
	}
}

func (t *testMemberlist) TestBroadcast() {
	lci := t.newConnInfo()
	lnode := base.RandomAddress("")
	rci := t.newConnInfo()
	rnode := base.RandomAddress("")

	addrs := []*net.UDPAddr{lci.UDPAddr(), rci.UDPAddr()}
	sort.Slice(addrs, func(i, j int) bool {
		return strings.Compare(addrs[i].String(), addrs[j].String()) < 0
	})

	ljoinedch := make(chan Member, 1)
	rjoinedch := make(chan Member, 1)
	_, lsrv, lstartf, lstopf := t.newServersForJoining(
		lnode,
		lci,
		func(node Member) {
			if isEqualAddress(node, lci) {
				return
			}

			ljoinedch <- node
		},
		nil,
	)

	_, rsrv, rstartf, rstopf := t.newServersForJoining(
		rnode,
		rci,
		func(node Member) {
			if isEqualAddress(node, rci) {
				return
			}

			rjoinedch <- node
		},
		nil,
	)

	rbroadcastedch := make(chan []byte, 1)

	rsrv.SetNotifyMsg(func(b []byte, _ encoder.Encoder) {
		rbroadcastedch <- b
	})

	t.NoError(lstartf())
	defer lstopf()
	t.NoError(rstartf())
	defer rstopf()

	<-time.After(time.Second)
	t.checkJoined(lsrv, rsrv, ljoinedch, rjoinedch)
	t.T().Log("joined")

	uid := util.UUID()

	t.T().Log("broadcasted:", uid)
	lsrv.Broadcast(NewBroadcast(uid.Bytes(), uid.String(), nil))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("remote failed to be notify msg"))
	case b := <-rbroadcastedch:
		t.Equal(uid.Bytes(), b)
	}
}

func (t *testMemberlist) TestCallbackBroadcast() {
	lci := t.newConnInfo()
	lnode := base.RandomAddress("")
	rci := t.newConnInfo()
	rnode := base.RandomAddress("")

	ljoinedch := make(chan Member, 1)
	rjoinedch := make(chan Member, 1)
	lph, lsrv, lstartf, lstopf := t.newServersForJoining(
		lnode,
		lci,
		func(node Member) {
			if isEqualAddress(node, lci) {
				return
			}

			ljoinedch <- node
		},
		nil,
	)

	_, rsrv, rstartf, rstopf := t.newServersForJoining(
		rnode,
		rci,
		func(node Member) {
			if isEqualAddress(node, rci) {
				return
			}

			rjoinedch <- node
		},
		nil,
	)

	callbackhandlerprefix := quicstream.HashPrefix("cb")

	lph.Add(callbackhandlerprefix, quicstreamheader.NewHandler(t.encs, 0, lsrv.CallbackBroadcastHandler(), nil))

	lcl := quicstreamheader.NewClient(t.encs, t.enc, func(
		ctx context.Context,
		addr quicstream.ConnInfo,
	) (io.Reader, io.WriteCloser, error) {
		return t.NewClient(addr.UDPAddr()).OpenStream(ctx)
	})

	notfoundid := util.UUID().String()

	rsrv.args.FetchCallbackBroadcastMessageFunc = func(ctx context.Context, m ConnInfoBroadcastMessage) (
		[]byte, encoder.Encoder, error,
	) {
		if m.ID() == notfoundid {
			return nil, nil, nil
		}

		return FetchCallbackBroadcastMessageFunc(callbackhandlerprefix, lcl.Broker)(ctx, m)
	}

	rbroadcastedch := make(chan []byte, 1)

	rsrv.SetNotifyMsg(func(b []byte, _ encoder.Encoder) {
		rbroadcastedch <- b
	})

	t.NoError(lstartf())
	defer lstopf()
	t.NoError(rstartf())
	defer rstopf()

	<-time.After(time.Second)
	t.checkJoined(lsrv, rsrv, ljoinedch, rjoinedch)
	t.T().Log("joined")

	t.Run("found", func() {
		uid := util.UUID()

		t.T().Log("broadcasted:", uid)
		t.NoError(lsrv.CallbackBroadcast(uid.Bytes(), uid.String(), nil))

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("remote failed to be notify msg"))
		case b := <-rbroadcastedch:
			t.T().Log("received:", b)
			t.Equal(uid.Bytes(), b)
		}

		after := time.After(time.Second * 3)
		for { // NOTE exhaust retransmitted messages
			select {
			case <-after:
				return
			case <-rbroadcastedch:
			}
		}
	})

	t.Run("not found", func() {
		t.T().Log("broadcasted")
		t.NoError(lsrv.CallbackBroadcast(util.UUID().Bytes(), notfoundid, nil))

		select {
		case <-time.After(time.Second * 2):
		case b := <-rbroadcastedch:
			t.T().Log("received:", b)
			t.NoError(errors.Errorf("unexpected callback broadcasted message"))
		}
	})
}

func (t *testMemberlist) TestEnsureBroadcast() {
	lci := t.newConnInfo()
	lnode := base.RandomLocalNode()
	t.T().Log("local conninfo:", lci)

	handlerprefix := quicstream.HashPrefix("eb")
	networkID := base.NetworkID(util.UUID().Bytes())

	rcis := make([]quicstream.ConnInfo, 3)
	rnodes := make([]base.LocalNode, len(rcis))
	rsrvs := make([]*Memberlist, len(rcis))
	rstartfs := make([]func() error, len(rcis))
	rstopfs := make([]func(), len(rcis))
	rjoinedchs := make([]chan struct{}, len(rcis))
	rnotifys := make([]util.LockedMap[string, []byte], len(rcis))

	ljoinedch := make(chan struct{}, 1)
	lph, lsrv, lstartf, lstopf := t.newServersForJoining(
		lnode.Address(),
		lci,
		func() func(node Member) {
			agg, _ := util.NewLockedMap[string, int](1)

			return func(node Member) {
				if isEqualAddress(node, lci) {
					return
				}

				agg.SetValue(node.Address().String(), 0)

				if agg.Len() == len(rcis) {
					ljoinedch <- struct{}{}
				}
			}
		}(),
		nil,
	)

	lcl := quicstreamheader.NewClient(t.encs, t.enc, func(
		ctx context.Context,
		addr quicstream.ConnInfo,
	) (io.Reader, io.WriteCloser, error) {
		return t.NewClient(addr.UDPAddr()).OpenStream(ctx)
	})

	for i := range rcis {
		i := i

		rci := t.newConnInfo()
		rcis[i] = rci
		node := base.RandomLocalNode()
		rnodes[i] = node

		ch := make(chan struct{}, 1)
		rjoinedchs[i] = ch

		_, rsrvs[i], rstartfs[i], rstopfs[i] = t.newServersForJoining(
			node.Address(),
			rcis[i],
			func() func(node Member) {
				agg, _ := util.NewLockedMap[string, int](1)

				return func(node Member) {
					if isEqualAddress(node, rci) {
						return
					}

					agg.SetValue(node.Address().String(), 0)

					if agg.Len() == len(rcis) {
						ch <- struct{}{}
					}
				}
			}(),
			nil,
		)

		rsrvs[i].args.PongEnsureBroadcastMessageFunc = func(ctx context.Context, m ConnInfoBroadcastMessage) error {
			if strings.HasPrefix(m.ID(), "not_ok") && i == 0 {
				return nil
			}

			return PongEnsureBroadcastMessageFunc(
				handlerprefix,
				node.Address(),
				node.Privatekey(),
				networkID,
				lcl.Broker,
			)(ctx, m)
		}

		m, err := util.NewLockedMap[string, []byte](1)
		t.NoError(err)
		rnotifys[i] = m

		rsrvs[i].SetNotifyMsg(func(b []byte, _ encoder.Encoder) {
			m.SetValue(string(b), nil)
		})
	}

	checkNotifyMsg := func(expected string, threshold float64, timeout time.Duration) error {
		th := base.Threshold(threshold)

		ticker := time.NewTicker(time.Millisecond * 333)
		defer ticker.Stop()

		after := time.After(timeout)

		for {
			select {
			case <-after:
				return errors.Errorf("failed to wait ensured")
			case <-ticker.C:
				var count uint
				for i := range rnotifys {
					_, found := rnotifys[i].Value(expected)
					if found {
						count++
					}
				}

				if count >= th.Threshold(uint(len(rnotifys))) {
					return nil
				}
			}
		}
	}

	lph.Add(handlerprefix, quicstreamheader.NewHandler(t.encs, 0, lsrv.EnsureBroadcastHandler(
		networkID,
		func(node base.Address) (base.Publickey, bool, error) {
			for i := range rnodes {
				r := rnodes[i]

				if r.Address().Equal(node) {
					return r.Publickey(), true, nil
				}
			}
			return nil, false, nil
		},
	), nil))

	t.NoError(lstartf())
	defer lstopf()

	for i := range rstartfs {
		t.NoError(rstartfs[i]())
		defer rstopfs[i]()
	}

	<-time.After(time.Second)
	t.checkJoined2(lsrv, ljoinedch, rsrvs, rjoinedchs)
	t.T().Log("joined")

	t.Run("ok:100", func() {
		notifych := make(chan error, 1)

		body := util.UUID().Bytes()
		t.NoError(lsrv.EnsureBroadcast(body, "ok:100", notifych,
			func(uint64) time.Duration { return time.Millisecond * 666 },
			100, 9))

		select {
		case <-time.After(time.Second * 6):
			t.NoError(errors.Errorf("failed to wait ensured"))
		case err := <-notifych:
			t.NoError(err)

			t.T().Log("ensured")
		}

		t.T().Log("checking broadcasted body")

		t.NoError(checkNotifyMsg(string(body), 100, time.Second*2))
	})

	t.Run("ok:60", func() {
		notifych := make(chan error, 1)

		body := util.UUID().Bytes()
		t.NoError(lsrv.EnsureBroadcast(body, "ok:60", notifych,
			func(uint64) time.Duration { return time.Millisecond * 666 },
			60, 9))

		select {
		case <-time.After(time.Second * 6):
			t.NoError(errors.Errorf("failed to wait ensured"))
		case err := <-notifych:
			t.NoError(err)

			t.T().Log("ensured")
		}

		t.T().Log("checking broadcasted body")

		t.NoError(checkNotifyMsg(string(body), 60, time.Second*2))
	})

	t.Run("not ok:100", func() {
		notifych := make(chan error, 1)

		t.NoError(lsrv.EnsureBroadcast(nil, "not_ok:100", notifych,
			func(uint64) time.Duration { return time.Millisecond * 666 },
			100, 9))

		select {
		case <-time.After(time.Second * 15):
			t.NoError(errors.Errorf("failed to wait ensured"))
		case err := <-notifych:
			t.Error(err)
			t.ErrorContains(err, "over max retry")

			t.T().Log("not ensured")
		}
	})

	t.Run("not ok:70", func() {
		notifych := make(chan error, 1)

		t.NoError(lsrv.EnsureBroadcast(nil, "not_ok:70", notifych,
			func(uint64) time.Duration { return time.Millisecond * 666 },
			70, 9))

		select {
		case <-time.After(time.Second * 15):
			t.NoError(errors.Errorf("failed to wait ensured"))
		case err := <-notifych:
			t.Error(err)
			t.ErrorContains(err, "over max retry")

			t.T().Log("not ensured")
		}
	})
}

func TestMemberlist(t *testing.T) {
	suite.Run(t, new(testMemberlist))
}
