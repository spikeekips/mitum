package quicmemberlist

import (
	"crypto/tls"
	"io"
	"net"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testMemberlist struct {
	quicstream.BaseTest
	enc *jsonenc.Encoder
}

func (t *testMemberlist) SetupTest() {
	t.BaseTest.SetupTest()
	t.enc = jsonenc.NewEncoder()

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: NodeHint, Instance: BaseNode{}}))
}

func (t *testMemberlist) newConnInfo() quicstream.UDPConnInfo {
	addr := t.BaseTest.NewBind()

	return quicstream.NewUDPConnInfo(addr, true)
}

func (t *testMemberlist) TestNew() {
	bind := t.NewBind()
	config := BasicMemberlistConfig(bind.String(), bind, bind)

	local, err := NewNode(bind.String(), bind, base.RandomAddress(""), base.NewMPrivatekey().Publickey(), "1.2.3.4:4321", true)
	t.NoError(err)

	config.Delegate = NewDelegate(local, nil, nil)
	config.Transport = &Transport{}
	config.Alive = NewAliveDelegate(t.enc, local.UDPAddr(), nil, nil)

	srv, err := NewMemberlist(local, t.enc, config, 3)
	t.NoError(err)

	t.NoError(srv.Start())
	defer t.NoError(srv.Stop())
}

func (t *testMemberlist) newServersForJoining(
	node base.Address,
	ci quicstream.UDPConnInfo,
	whenJoined DelegateJoinedFunc,
	whenLeft DelegateLeftFunc,
) (*quicstream.Server, *Memberlist) {
	tlsconfig := t.NewTLSConfig(t.Proto)

	poolclient := quicstream.NewPoolClient()

	laddr := ci.UDPAddr()
	transport := NewTransportWithQuicstream(
		laddr,
		"",
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
	)

	handler := func(addr net.Addr, r io.Reader, w io.Writer) error {
		b, err := io.ReadAll(r)
		if err != nil {
			return err
		}

		if err := transport.ReceiveRaw(b, addr); err != nil {
			return err
		}

		return nil
	}

	quicstreamsrv := quicstream.NewServer(laddr, tlsconfig, nil, handler)

	local, err := NewNode(laddr.String(), laddr, node, base.NewMPrivatekey().Publickey(), "1.2.3.4:4321", true)
	t.NoError(err)

	memberlistconfig := BasicMemberlistConfig(local.Name(), laddr, laddr)
	memberlistconfig.Transport = transport
	memberlistconfig.Events = NewEventsDelegate(t.enc, whenJoined, whenLeft)
	memberlistconfig.Alive = NewAliveDelegate(t.enc, laddr, func(Node) error { return nil }, func(Node) error { return nil })

	memberlistconfig.Delegate = NewDelegate(local, nil, nil)

	srv, _ := NewMemberlist(local, t.enc, memberlistconfig, 3)

	return quicstreamsrv, srv
}

func (t *testMemberlist) TestLocalJoinAlone() {
	lci := t.newConnInfo()
	lnode := base.RandomAddress("")

	joinedch := make(chan Node, 1)
	quicstreamsrv, srv := t.newServersForJoining(
		lnode,
		lci,
		func(node Node) {
			joinedch <- node
		},
		nil,
	)

	t.NoError(quicstreamsrv.Start())
	defer quicstreamsrv.Stop()

	t.NoError(srv.Start())
	defer srv.Stop()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("local failed to join"))
	case node := <-joinedch:
		t.True(isEqualAddress(lci, node))
	}

	t.Equal(1, srv.MembersLen())

	var joined []Node
	srv.Members(func(node Node) bool {
		joined = append(joined, node)

		return true
	})
	t.Equal(1, len(joined))
	t.True(isEqualAddress(lci, joined[0]))
}

func (t *testMemberlist) TestLocalJoinAloneAndRejoin() {
	lci := t.newConnInfo()
	lnode := base.RandomAddress("")

	joinedch := make(chan Node, 1)
	leftch := make(chan Node, 1)
	quicstreamsrv, srv := t.newServersForJoining(
		lnode,
		lci,
		func(node Node) {
			joinedch <- node
		},
		func(node Node) {
			leftch <- node
		},
	)

	t.NoError(quicstreamsrv.Start())
	defer quicstreamsrv.Stop()

	t.NoError(srv.Start())
	defer srv.Stop()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("local failed to join"))
	case node := <-joinedch:
		t.True(isEqualAddress(lci, node))
	}

	t.Equal(1, srv.MembersLen())

	var joined []Node
	srv.Members(func(node Node) bool {
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

	ljoinedch := make(chan Node, 1)
	rjoinedch := make(chan Node, 1)
	lqsrv, lsrv := t.newServersForJoining(
		lnode,
		lci,
		func(node Node) {
			if isEqualAddress(node, lci) {
				return
			}

			ljoinedch <- node
		},
		nil,
	)

	rqsrv, rsrv := t.newServersForJoining(
		rnode,
		rci,
		func(node Node) {
			if isEqualAddress(node, rci) {
				return
			}

			rjoinedch <- node
		},
		nil,
	)

	t.NoError(lqsrv.Start())
	t.NoError(rqsrv.Start())
	defer lqsrv.Stop()
	defer rqsrv.Stop()

	t.NoError(lsrv.Start())
	t.NoError(rsrv.Start())
	defer lsrv.Stop()
	defer rsrv.Stop()

	<-time.After(time.Second)
	t.NoError(lsrv.Join([]quicstream.UDPConnInfo{rci}))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("local failed to join to remote"))
	case node := <-ljoinedch:
		t.True(isEqualAddress(rci, node))

		t.Equal(2, lsrv.MembersLen())

		var joined []Node
		lsrv.Members(func(node Node) bool {
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

		var joined []Node
		rsrv.Members(func(node Node) bool {
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

	rjoinedch := make(chan Node, 1)
	lqsrv, lsrv := t.newServersForJoining(
		lnode,
		lci,
		nil,
		nil,
	)

	lsrv.mconfig.Alive = NewAliveDelegate(
		t.enc,
		lci.UDPAddr(),
		func(node Node) error {
			if isEqualAddress(node, rci) {
				return errors.Errorf("failed to challenge")
			}

			return nil
		},
		func(node Node) error { return nil },
	)
	lsrv, _ = NewMemberlist(lsrv.local, t.enc, lsrv.mconfig, 3)

	rqsrv, rsrv := t.newServersForJoining(
		rnode,
		rci,
		func(node Node) {
			if isEqualAddress(node, rci) {
				return
			}

			rjoinedch <- node
		},
		nil,
	)

	t.NoError(lqsrv.Start())
	t.NoError(rqsrv.Start())
	defer lqsrv.Stop()
	defer rqsrv.Stop()

	t.NoError(lsrv.Start())
	t.NoError(rsrv.Start())
	defer lsrv.Stop()
	defer rsrv.Stop()

	<-time.After(time.Second)
	err := lsrv.Join([]quicstream.UDPConnInfo{rci})
	t.Error(err)
	t.True(errors.Is(err, ErrNotYetJoined))

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

	rjoinedch := make(chan Node, 1)
	lqsrv, lsrv := t.newServersForJoining(
		lnode,
		lci,
		nil,
		nil,
	)

	lsrv.mconfig.Alive = NewAliveDelegate(
		t.enc,
		lci.UDPAddr(),
		func(node Node) error { return nil },
		func(node Node) error {
			if isEqualAddress(node, rci) {
				return errors.Errorf("remote disallowed")
			}

			return nil
		},
	)
	lsrv, _ = NewMemberlist(lsrv.local, t.enc, lsrv.mconfig, 3)

	rqsrv, rsrv := t.newServersForJoining(
		rnode,
		rci,
		func(node Node) {
			if isEqualAddress(node, rci) {
				return
			}

			rjoinedch <- node
		},
		nil,
	)

	t.NoError(lqsrv.Start())
	t.NoError(rqsrv.Start())
	defer lqsrv.Stop()
	defer rqsrv.Stop()

	t.NoError(lsrv.Start())
	t.NoError(rsrv.Start())
	defer lsrv.Stop()
	defer rsrv.Stop()

	<-time.After(time.Second)
	err := lsrv.Join([]quicstream.UDPConnInfo{rci})
	t.Error(err)
	t.True(errors.Is(err, ErrNotYetJoined))

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

	ljoinedch := make(chan Node, 3)
	rjoinedch := make(chan Node, 3)

	lleftch := make(chan Node, 3)
	rleftch := make(chan Node, 3)
	lqsrv, lsrv := t.newServersForJoining(
		lnode,
		lci,
		func(node Node) {
			if isEqualAddress(node, lci) {
				return
			}

			ljoinedch <- node
		},
		func(node Node) {
			lleftch <- node
		},
	)

	rqsrv, rsrv := t.newServersForJoining(
		rnode,
		rci,
		func(node Node) {
			if isEqualAddress(node, rci) {
				return
			}

			rjoinedch <- node
		},
		func(node Node) {
			rleftch <- node
		},
	)

	t.NoError(lqsrv.Start())
	t.NoError(rqsrv.Start())
	defer lqsrv.Stop()
	defer rqsrv.Stop()

	t.NoError(lsrv.Start())
	t.NoError(rsrv.Start())
	defer lsrv.Stop()
	defer rsrv.Stop()

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

	ljoinedch := make(chan Node, 1)
	rjoinedch := make(chan Node, 1)

	rleftch := make(chan Node, 1)
	lqsrv, lsrv := t.newServersForJoining(
		lnode,
		lci,
		func(node Node) {
			if isEqualAddress(node, lci) {
				return
			}

			ljoinedch <- node
		},
		nil,
	)

	rqsrv, rsrv := t.newServersForJoining(
		rnode,
		rci,
		func(node Node) {
			if isEqualAddress(node, rci) {
				return
			}

			rjoinedch <- node
		},
		func(node Node) {
			rleftch <- node
		},
	)

	t.NoError(lqsrv.Start())
	t.NoError(rqsrv.Start())
	defer lqsrv.Stop()
	defer rqsrv.Stop()

	t.NoError(lsrv.Start())
	t.NoError(rsrv.Start())
	defer lsrv.Stop()
	defer rsrv.Stop()

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

	ljoinedch := make(chan Node, 1)
	lqsrv, lsrv := t.newServersForJoining(
		lnode,
		lci,
		func(node Node) {
			if isEqualAddress(node, lci) {
				return
			}

			ljoinedch <- node
		},
		nil,
	)

	lsrv.sameMemberLimit = 3

	rqsrv0, rsrv0 := t.newServersForJoining(rnode, rci0, nil, nil)
	rqsrv1, rsrv1 := t.newServersForJoining(rnode, rci1, nil, nil)

	t.NoError(lqsrv.Start())
	t.NoError(rqsrv0.Start())
	t.NoError(rqsrv1.Start())
	defer lqsrv.Stop()
	defer rqsrv0.Stop()
	defer rqsrv1.Stop()

	t.NoError(lsrv.Start())
	t.NoError(rsrv0.Start())
	t.NoError(rsrv1.Start())
	defer lsrv.Stop()
	defer rsrv0.Stop()
	defer rsrv1.Stop()

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
	t.Equal(2, lsrv.members.NodesLen(rnode))
}

func (t *testMemberlist) TestLocalOverMemberLimit() {
	lci := t.newConnInfo()
	lnode := base.RandomAddress("")

	rci0 := t.newConnInfo()
	rnode := base.RandomAddress("")

	ljoinedch := make(chan Node, 1)
	lqsrv, lsrv := t.newServersForJoining(
		lnode,
		lci,
		func(node Node) {
			if isEqualAddress(node, lci) {
				return
			}

			ljoinedch <- node
		},
		nil,
	)

	lsrv.sameMemberLimit = 0 // NOTE only allow 1 member in node name

	rqsrv0, rsrv0 := t.newServersForJoining(
		rnode,
		rci0,
		nil,
		nil,
	)

	t.NoError(lqsrv.Start())
	t.NoError(rqsrv0.Start())
	defer lqsrv.Stop()
	defer rqsrv0.Stop()

	t.NoError(lsrv.Start())
	t.NoError(rsrv0.Start())
	defer lsrv.Stop()
	defer rsrv0.Stop()

	<-time.After(time.Second)
	t.NoError(lsrv.Join([]quicstream.UDPConnInfo{rci0}))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("local failed to join to remote"))
	case node := <-ljoinedch:
		t.True(isEqualAddress(rci0, node))

		t.Equal(2, lsrv.MembersLen())
		t.Equal(1, lsrv.members.NodesLen(rnode))
	}

	t.T().Log("new remote node trying to join to local")

	rci1 := t.newConnInfo()

	rqsrv1, rsrv1 := t.newServersForJoining(
		rnode,
		rci1,
		nil,
		nil,
	)

	t.NoError(rqsrv1.Start())
	t.NoError(rsrv1.Start())
	defer rqsrv1.Stop()
	defer rsrv1.Stop()

	<-time.After(time.Second)
	t.NoError(rsrv1.Join([]quicstream.UDPConnInfo{lci}))

	<-time.After(time.Second * 3)
	t.Equal(2, lsrv.MembersLen())

	t.Equal(1, lsrv.members.NodesLen(rnode))

	var joinedremotes []Node
	lsrv.Members(func(node Node) bool {
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

	ljoinedch := make(chan Node, 1)
	rjoinedch := make(chan Node, 1)
	lqsrv, lsrv := t.newServersForJoining(
		lnode,
		lci,
		func(node Node) {
			if isEqualAddress(node, lci) {
				return
			}

			ljoinedch <- node
		},
		nil,
	)

	rqsrv, rsrv := t.newServersForJoining(
		rnode,
		rci,
		func(node Node) {
			if isEqualAddress(node, rci) {
				return
			}

			rjoinedch <- node
		},
		nil,
	)

	remote, err := NewNode(rci.UDPAddr().String(), rci.UDPAddr(), rnode, base.NewMPrivatekey().Publickey(), "", true) // NOTE empty publish
	t.NoError(err)
	err = remote.IsValid(nil)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "empty publish")

	rdelegate := rsrv.mconfig.Delegate.(*Delegate)
	rdelegate.local = remote

	t.NoError(lqsrv.Start())
	t.NoError(rqsrv.Start())
	defer lqsrv.Stop()
	defer rqsrv.Stop()

	t.NoError(lsrv.Start())
	t.NoError(rsrv.Start())
	defer lsrv.Stop()
	defer rsrv.Stop()

	<-time.After(time.Second)
	err = lsrv.Join([]quicstream.UDPConnInfo{rci})
	t.Error(err)
	t.True(errors.Is(err, ErrNotYetJoined))

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

	lqsrv, lsrv := t.newServersForJoining(
		lnode,
		lci,
		func(node Node) {},
		nil,
	)

	t.NoError(lqsrv.Start())
	defer lqsrv.Stop()

	t.NoError(lsrv.Start())
	defer lsrv.Stop()

	err := lsrv.Join([]quicstream.UDPConnInfo{rci})
	t.Error(err)
	t.False(errors.Is(err, ErrNotYetJoined))
}

func TestMemberlist(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/lucas-clemente/quic-go.(*client).dial"),
	)

	suite.Run(t, new(testMemberlist))
}
