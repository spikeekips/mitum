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
		return NewItemReader(t, enc, r, workerSize)
	}
}

func NewItemReader(
	t base.BlockItemType,
	enc encoder.Encoder,
	r *util.CompressedReader,
	workerSize uint64,
) (isaac.BlockItemReader, error) {
	return &ItemReader{
		t:             t,
		enc:           enc,
		r:             r,
		maxWorkerSize: workerSize,
	}, nil
}

func (r *ItemReader) Type() base.BlockItemType {
	return r.t
}

func (r *ItemReader) Encoder() encoder.Encoder {
	return r.enc
}

func (r *ItemReader) Reader() *util.CompressedReader {
	return r.r
}

func (r *ItemReader) Decode() (interface{}, error) {
	r.Lock()
	defer r.Unlock()

	if err := r.decompressed(); err != nil {
		return nil, err
	}

	switch r.t {
	case base.BlockItemMap, base.BlockItemProposal:
		return r.decodeOneItem()
	case base.BlockItemOperations, base.BlockItemStates:
		i, n, err := r.decodeCountItems(nil)
		if err != nil {
			return nil, err
		}

		return i[:n], nil
	case base.BlockItemOperationsTree,
		base.BlockItemStatesTree:
		return r.decodeTree(nil)
	case base.BlockItemVoteproofs:
		return r.decodeVoteproofs()
	default:
		return nil, errors.Errorf("unknown block item, %q", r.t)
	}
}

func (r *ItemReader) DecodeItems(f func(uint64, uint64, interface{}) error) (uint64, error) {
	r.Lock()
	defer r.Unlock()

	if err := r.decompressed(); err != nil {
		return 0, err
	}

	switch r.t {
	case base.BlockItemMap,
		base.BlockItemProposal,
		base.BlockItemVoteproofs,
		base.BlockItemStatesTree,
		base.BlockItemStatesTree:
		return 0, errors.Errorf("unsupported items type, %q", r.t)
	case base.BlockItemOperations, base.BlockItemStates:
		_, n, err := r.decodeCountItems(f)

		return n, err
	default:
		return 0, errors.Errorf("unknown block item, %q", r.t)
	}
}

func (r *ItemReader) decodeOneItem() (interface{}, error) {
	var br *bufio.Reader

	switch i, _, err := readItemsHeader(r.dr); {
	case err != nil:
		return nil, err
	default:
		br = i
	}

	var u interface{}

	if err := encoder.DecodeReader(r.enc, br, &u); err != nil {
		return nil, err
	}

	return u, nil
}

func (r *ItemReader) decodeCountItems(item func(uint64, uint64, interface{}) error) ([]interface{}, uint64, error) {
	var l []interface{}

	if item == nil {
		var once sync.Once

		item = func(total uint64, index uint64, v interface{}) error { //revive:disable-line:modifies-parameter
			once.Do(func() {
				l = make([]interface{}, total)
			})

			l[index] = v

			return nil
		}
	}

	var br *bufio.Reader
	var u countItemsHeader

	switch i, err := loadItemsHeader(r.dr, &u); {
	case err != nil:
		return nil, 0, err
	default:
		br = i
	}

	if u.Count < 1 {
		return nil, 0, nil
	}

	workerSize := r.maxWorkerSize
	if u.Count < workerSize {
		workerSize = u.Count
	}

	switch count, err := DecodeLineItemsWithWorker(
		br, workerSize, r.enc.Decode,
		func(index uint64, v interface{}) error {
			return item(u.Count, index, v)
		},
	); {
	case err != nil:
		return nil, 0, err
	default:
		if l != nil {
			l = l[:count]
		}

		return l, count, nil
	}
}

func (r *ItemReader) decodeTree(item func(uint64, uint64, interface{}) error) (tr fixedtree.Tree, _ error) {
	if item == nil {
		item = func(uint64, uint64, interface{}) error { return nil } //revive:disable-line:modifies-parameter
	}

	var br *bufio.Reader
	var u treeItemsHeader

	switch i, err := loadItemsHeader(r.dr, &u); {
	case err != nil:
		return tr, err
	default:
		br = i
	}

	if u.Count < 1 {
		return tr, nil
	}

	workerSize := r.maxWorkerSize
	if u.Count < workerSize {
		workerSize = u.Count
	}

	nodes := make([]fixedtree.Node, u.Count)

	switch i, err := fixedtree.NewTree(u.Tree, nodes); {
	case err != nil:
		return tr, err
	default:
		tr = i
	}

	if _, err := DecodeLineItemsWithWorker(
		br,
		workerSize,
		func(b []byte) (interface{}, error) {
			return unmarshalIndexedTreeNode(r.enc, b, u.Tree)
		},
		func(index uint64, v interface{}) error {
			in, ok := v.(indexedTreeNode) //nolint:forcetypeassert //...
			if !ok {
				return errors.Errorf("not indexedTreeNode, %T", v)
			}

			if err := item(u.Count, in.Index, in.Node); err != nil {
				return err
			}

			return tr.Set(in.Index, in.Node)
		},
	); err != nil {
		return tr, err
	}

	return tr, nil
}

func (r *ItemReader) decodeVoteproofs() (interface{}, error) {
	var br *bufio.Reader

	switch i, _, err := readItemsHeader(r.dr); {
	case err != nil:
		return nil, err
	default:
		br = i
	}

	var vps [2]base.Voteproof

	if _, err := DecodeLineItemsWithWorker(br, 2, r.enc.Decode, func(_ uint64, v interface{}) error {
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
		return nil, err
	}

	if vps[0] == nil || vps[1] == nil {
		return nil, errors.Errorf("missing")
	}

	return vps, nil
}

func (r *ItemReader) decompressed() error {
	if r.dr == nil {
		switch i, err := r.r.Decompress(); {
		case err != nil:
			return err
		default:
			r.dr = i
		}
	}

	return nil
}
