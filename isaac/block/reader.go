package isaacblock

import (
	"bufio"
	"io"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
)

type ItemReader struct {
	dr            io.Reader
	enc           encoder.Encoder
	r             *util.CompressedReader
	t             base.BlockItemType
	maxWorkerSize uint64
	sync.Mutex
}

func NewDefaultItemReaderFunc(workerSize uint64) NewItemReaderFunc {
	return func(
		t base.BlockItemType,
		enc encoder.Encoder,
		r *util.CompressedReader,
	) (isaac.BlockItemReader, error) {
		return &ItemReader{
			t:             t,
			enc:           enc,
			r:             r,
			maxWorkerSize: workerSize,
		}, nil
	}
}

func (r *ItemReader) Type() base.BlockItemType {
	return r.t
}

func (r *ItemReader) Reader() *util.CompressedReader {
	return r.r
}

func (r *ItemReader) Decode(f func(interface{}) error) error {
	r.Lock()
	defer r.Unlock()

	if r.dr == nil {
		switch i, err := r.r.Decompress(); {
		case err != nil:
			return err
		default:
			r.dr = i
		}
	}

	switch r.t {
	case base.BlockItemMap, base.BlockItemProposal:
		return r.decodeOneItem(f)
	case base.BlockItemOperations, base.BlockItemStates:
		return r.decodeCountItems(f)
	case base.BlockItemOperationsTree,
		base.BlockItemStatesTree:
		return r.decodeTree(f)
	case base.BlockItemVoteproofs:
		return r.decodeVoteproofs(f)
	default:
		return errors.Errorf("unknown block item, %q", r.t)
	}
}

func (r *ItemReader) decodeOneItem(f func(interface{}) error) error {
	var br *bufio.Reader

	switch i, _, err := readItemsHeader(r.dr); {
	case err != nil:
		return err
	default:
		br = i
	}

	var u interface{}

	if err := encoder.DecodeReader(r.enc, br, &u); err != nil {
		return err
	}

	return f(u)
}

func (r *ItemReader) decodeCountItems(f func(interface{}) error) error {
	var br *bufio.Reader
	var u countItemsHeader

	switch i, err := loadItemsHeader(r.dr, &u); {
	case err != nil:
		return err
	default:
		br = i
	}

	if u.Count < 1 {
		return f(nil)
	}

	workerSize := r.maxWorkerSize
	if u.Count < workerSize {
		workerSize = u.Count
	}

	l := make([]interface{}, u.Count)

	if err := DecodeLineItemsWithWorker(br, workerSize, r.enc.Decode, func(index uint64, v interface{}) error {
		l[index] = v

		return nil
	}); err != nil {
		return err
	}

	return f(l)
}

func (r *ItemReader) decodeTree(f func(interface{}) error) error {
	var br *bufio.Reader
	var u treeItemsHeader

	switch i, err := loadItemsHeader(r.dr, &u); {
	case err != nil:
		return err
	default:
		br = i
	}

	if u.Count < 1 {
		return f(nil)
	}

	workerSize := r.maxWorkerSize
	if u.Count < workerSize {
		workerSize = u.Count
	}

	nodes := make([]fixedtree.Node, u.Count)

	var tr fixedtree.Tree

	switch i, err := fixedtree.NewTree(u.Tree, nodes); {
	case err != nil:
		return err
	default:
		tr = i
	}

	if err := DecodeLineItemsWithWorker(
		br,
		workerSize,
		func(b []byte) (interface{}, error) {
			return unmarshalIndexedTreeNode(r.enc, b, u.Tree)
		},
		func(_ uint64, v interface{}) error {
			in, ok := v.(indexedTreeNode) //nolint:forcetypeassert //...
			if !ok {
				return errors.Errorf("not indexedTreeNode, %T", v)
			}

			return tr.Set(in.Index, in.Node)
		},
	); err != nil {
		return err
	}

	return f(tr)
}

func (r *ItemReader) decodeVoteproofs(f func(interface{}) error) error {
	var br *bufio.Reader

	switch i, _, err := readItemsHeader(r.dr); {
	case err != nil:
		return err
	default:
		br = i
	}

	var vps [2]base.Voteproof

	if err := DecodeLineItemsWithWorker(br, 2, r.enc.Decode, func(_ uint64, v interface{}) error {
		switch t := v.(type) {
		case base.INITVoteproof:
			vps[0] = t
		case base.ACCEPTVoteproof:
			vps[1] = t
		default:
			return errors.Errorf("not voteproof, %T", v)
		}

		return nil
	}); err != nil {
		return err
	}

	if vps[0] == nil || vps[1] == nil {
		return errors.Errorf("missing")
	}

	return f(vps)
}
