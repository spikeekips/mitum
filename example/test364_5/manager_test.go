package main

import (
	"slices"
	"strings"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	"github.com/spikeekips/mitum/util"
)

type baseTestManager struct{}

func (t *baseTestManager) checkOpsTree(ophs [][2]util.Hash, expectedReasons map[uint64]string, tr *fixedtree.Tree) error {
	if err := tr.IsValid(nil); err != nil {
		return err
	}

	if len(ophs) != tr.Len() {
		return errors.Errorf("len ops, %d != tr.Len, %d", len(ophs), tr.Len())
	}

	return tr.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
		if err := n.IsValid(nil); err != nil {
			return false, err
		}

		op := ophs[index]
		b, ok := n.(*OperationTreeNode)
		if !ok {
			return false, errors.Errorf("expected *OperationTreeNode, but %T", n)
		}

		if b.Hash() == nil {
			return false, errors.Errorf("nil node hash")
		}

		if op[0].String() != b.Key() {
			return false, errors.Errorf("expected key, %q, but %q", op[0].String(), b.Key())
		}
		if !op[1].Equal(b.Fact()) {
			return false, errors.Errorf("expected fact, %q, but %q", op[1].String(), b.Fact())
		}

		if e, found := expectedReasons[index]; found {
			if e != b.Reason() {
				return false, errors.Errorf("expected reason, %q, but %q", e, b.Reason())
			}
		} else {
			if b.Reason() != "" {
				return false, errors.Errorf("expected empty reason, but %q", b.Reason())
			}
		}

		return true, nil
	})
}

func (t *baseTestManager) checkStatesTree(expectedStates [][2]interface{}, tr *fixedtree.Tree) error {
	if err := tr.IsValid(nil); err != nil {
		return err
	}

	if len(expectedStates) != tr.Len() {
		return errors.Errorf("expected, %d != tr.Len, %d", len(expectedStates), tr.Len())
	}

	slices.SortFunc(expectedStates, func(a, b [2]interface{}) int {
		return strings.Compare(a[0].(string), b[0].(string))
	})

	return tr.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
		a := expectedStates[index]
		b, ok := n.(*StateTreeNode)
		if !ok {
			return false, errors.Errorf("expected *StateTreeNode, but %T", n)
		}

		if e := a[0].(string); e != b.Key() {
			return false, errors.Errorf("expected key, %q, but %q", e, b.Key())
		}
		if e := a[1].(util.Hash); !e.Equal(b.StateHash()) {
			return false, errors.Errorf("expected state hash, %q, but %q", e, b.StateHash())
		}

		return true, nil
	})
}

func (t *baseTestManager) baseGenerateNodesHashFunc() fixedtree.GenerateNodesHashFunc {
	return fixedtree.NewFuncAsyncGenerateNodesHashByFollowingParent(1 << 9)
}
