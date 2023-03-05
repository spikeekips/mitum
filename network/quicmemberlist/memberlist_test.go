package quicmemberlist

import (
	"bytes"
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
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
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
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: CallbackBroadcastMessageHint, Instance: CallbackBroadcastMessage{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: CallbackBroadcastMessageHeaderHint, Instance: CallbackBroadcastMessageHeader{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: quicstream.DefaultResponseHeaderHint, Instance: quicstream.DefaultResponseHeader{}}))
}

func (t *testMemberlist) SetupTest() {
	t.BaseTest.SetupTest()
}

func (t *testMemberlist) newConnInfo() quicstream.UDPConnInfo {
	addr := t.BaseTest.NewBind()

	return quicstream.NewUDPConnInfo(addr, true)
}

func (t *testMemberlist) newargs(config *memberlist.Config) *MemberlistArgs {
	return NewMemberlistArgs(t.enc, config)
}

func (t *testMemberlist) TestNew() {
	bind := t.NewBind()
	config := BasicMemberlistConfig(bind.String(), bind, bind)

	local, err := NewMember(bind.String(), bind, base.RandomAddress(""), base.NewMPrivatekey().Publickey(), "1.2.3.4:4321", true)
	t.NoError(err)

	config.Delegate = NewDelegate(local, nil, nil)
	config.Transport = &Transport{args: NewTransportArgs()}
	config.Alive = NewAliveDelegate(t.enc, local.UDPAddr(), nil, nil)

	args := t.newargs(config)

	srv, err := NewMemberlist(local, args)
	t.NoError(err)

	t.NoError(srv.Start(context.Background()))
	defer t.NoError(srv.Stop())
}

func (t *testMemberlist) newServersForJoining(
	node base.Address,
	ci quicstream.UDPConnInfo,
	whenJoined DelegateJoinedFunc,
	whenLeft DelegateLeftFunc,
) (
	*quicstream.PrefixHandler,
	*Memberlist,
	func() error,
	func(),
) {
	transportprefix := "tp"
	tlsconfig := t.NewTLSConfig(t.Proto)

	poolclient := quicstream.NewPoolClient()

	laddr := ci.UDPAddr()

	transport := NewTransportWithQuicstream(
		laddr,
		transportprefix,
		poolclient,
		func(ci quicstream.UDPConnInfo) func(*net.UDPAddr) *quicstream.Client {
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
	)

	ph := quicstream.NewPrefixHandler(nil)
	ph.Add(transportprefix, transport.QuicstreamHandler)

	quicstreamsrv, err := quicstream.NewServer(laddr, tlsconfig, nil, ph.Handler)
	t.NoError(err)

	local, err := NewMember(laddr.String(), laddr, node, base.NewMPrivatekey().Publickey(), "1.2.3.4:4321", true)
	t.NoError(err)

	memberlistconfig := BasicMemberlistConfig(local.Name(), laddr, laddr)
	memberlistconfig.Transport = transport
	memberlistconfig.Events = NewEventsDelegate(t.enc, whenJoined, whenLeft)
	memberlistconfig.Alive = NewAliveDelegate(t.enc, laddr, func(Member) error { return nil }, func(Member) error { return nil })

	memberlistconfig.Delegate = NewDelegate(local, nil, nil)

	args := t.newargs(memberlistconfig)

	srv, _ := NewMemberlist(local, args)

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
		t.NoError(srv.Join([]quicstream.UDPConnInfo{lci}))

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
	t.NoError(lsrv.Join([]quicstream.UDPConnInfo{rci}))

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
			return strings.Compare(joined[i].UDPAddr().String(), joined[j].UDPAddr().String()) < 0
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
			return strings.Compare(joined[i].UDPAddr().String(), joined[j].UDPAddr().String()) < 0
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
	err := lsrv.Join([]quicstream.UDPConnInfo{rci})
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
	err := lsrv.Join([]quicstream.UDPConnInfo{rci})
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
	t.NoError(lsrv.Join([]quicstream.UDPConnInfo{rci}))

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
		t.NoError(lsrv.Join([]quicstream.UDPConnInfo{rci}))

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
	t.NoError(lsrv.Join([]quicstream.UDPConnInfo{rci}))

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

	lsrv.args.ExtraSameMemberLimit = 3

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
	t.NoError(lsrv.Join([]quicstream.UDPConnInfo{rci0, rci1}))

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

	lsrv.args.ExtraSameMemberLimit = 0 // NOTE only allow 1 member in node name

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
	t.NoError(lsrv.Join([]quicstream.UDPConnInfo{rci0}))

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
	t.NoError(rsrv1.Join([]quicstream.UDPConnInfo{lci}))

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
	t.True(isEqualAddress(rci0, joinedremotes[0].UDPAddr()))
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
	err = lsrv.Join([]quicstream.UDPConnInfo{rci})
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

	err := lsrv.Join([]quicstream.UDPConnInfo{rci})
	t.Error(err)
}

func (t *testMemberlist) checkJoined(
	lsrv, rsrv *Memberlist,
	ljoinedch, rjoinedch chan Member,
) {
	lci := lsrv.local.UDPConnInfo()
	rci := rsrv.local.UDPConnInfo()

	addrs := []*net.UDPAddr{lsrv.local.UDPAddr(), rsrv.local.UDPAddr()}
	sort.Slice(addrs, func(i, j int) bool {
		return strings.Compare(addrs[i].String(), addrs[j].String()) < 0
	})

	t.NoError(lsrv.Join([]quicstream.UDPConnInfo{rci}))

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
			return strings.Compare(joined[i].UDPAddr().String(), joined[j].UDPAddr().String()) < 0
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
			return strings.Compare(joined[i].UDPAddr().String(), joined[j].UDPAddr().String()) < 0
		})
		t.True(isEqualAddress(addrs[0], joined[0]))
		t.True(isEqualAddress(addrs[1], joined[1]))
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

	addrs := []*net.UDPAddr{lci.UDPAddr(), rci.UDPAddr()}
	sort.Slice(addrs, func(i, j int) bool {
		return strings.Compare(addrs[i].String(), addrs[j].String()) < 0
	})

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

	callbackhandlerprefix := "cb"

	lph.Add(callbackhandlerprefix, quicstream.NewHeaderHandler(t.encs, 0, lsrv.CallbackBroadcastHandler()))

	lcl := quicstream.NewHeaderClient(t.encs, t.enc, func(
		_ context.Context,
		_ quicstream.UDPConnInfo,
		writef quicstream.ClientWriteFunc,
	) (_ io.ReadCloser, cancel func() error, _ error) {
		r := bytes.NewBuffer(nil)
		if err := writef(r); err != nil {
			return nil, nil, err
		}

		w := bytes.NewBuffer(nil)
		if err := lph.Handler(lci.Addr(), r, w); err != nil {
			return nil, nil, err
		}

		return io.NopCloser(w), func() error { return nil }, nil
	})

	notfoundid := util.UUID().String()

	rsrv.args.FetchCallbackBroadcastMessageFunc = func(ctx context.Context, m CallbackBroadcastMessage) ([]byte, encoder.Encoder, error) {
		if m.ID() == notfoundid {
			return nil, nil, nil
		}

		h := NewCallbackBroadcastMessageHeader(m.ID(), callbackhandlerprefix)

		_, r, cancel, enc, err := lcl.RequestBody(ctx, m.ConnInfo(), h, nil)
		if err != nil {
			return nil, enc, err
		}

		defer cancel()

		b, err := io.ReadAll(r)
		if err != nil {
			return nil, enc, err
		}

		return b, enc, nil
	}

	rsrv.args.FetchCallbackBroadcastMessageFunc = func(ctx context.Context, m CallbackBroadcastMessage) (
		[]byte, encoder.Encoder, error,
	) {
		if m.ID() == notfoundid {
			return nil, nil, nil
		}

		return FetchCallbackBroadcastMessageFunc(callbackhandlerprefix, lcl.RequestBody)(ctx, m)
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

func TestMemberlist(t *testing.T) {
	suite.Run(t, new(testMemberlist))
}
