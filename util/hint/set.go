package hint

import (
	"github.com/bluele/gcache"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type CompatibleSet struct {
	set           map[Type]map[uint64]interface{}
	hints         map[Type]map[uint64]Hint
	typeheads     map[Type]interface{}
	typeheadhints map[Type]Hint
	cache         gcache.Cache
}

func NewCompatibleSet() *CompatibleSet {
	return &CompatibleSet{
		set:           map[Type]map[uint64]interface{}{},
		hints:         map[Type]map[uint64]Hint{},
		typeheads:     map[Type]interface{}{},
		typeheadhints: map[Type]Hint{},
		cache:         gcache.New(100 * 100).LRU().Build(), //nolint:gomnd //...
	}
}

func (st *CompatibleSet) Add(ht Hint, v interface{}) error {
	return st.add(ht, v)
}

func (st *CompatibleSet) AddHinter(hr Hinter) error {
	return st.add(hr.Hint(), hr)
}

func (st *CompatibleSet) add(ht Hint, v interface{}) error {
	if err := st.addWithHint(ht, v); err != nil {
		return errors.WithMessage(err, "failed to add to CompatibleSet")
	}

	switch eht, found := st.typeheadhints[ht.Type()]; {
	case !found:
		st.typeheads[ht.Type()] = v
		st.typeheadhints[ht.Type()] = ht
	case ht.Version().Compare(eht.Version()) > 0:
		st.typeheads[ht.Type()] = v
		st.typeheadhints[ht.Type()] = ht
	}

	return nil
}

func (st *CompatibleSet) addWithHint(ht Hint, v interface{}) error {
	if err := ht.IsValid(nil); err != nil {
		return errors.WithMessage(err, "failed to add with hint")
	}

	if _, found := st.set[ht.Type()]; !found {
		st.set[ht.Type()] = map[uint64]interface{}{ht.Version().Major(): v}
		st.hints[ht.Type()] = map[uint64]Hint{ht.Version().Major(): ht}

		return nil
	}

	eht, found := st.hints[ht.Type()][ht.Version().Major()]
	if !found {
		st.set[ht.Type()][ht.Version().Major()] = v
		st.hints[ht.Type()][ht.Version().Major()] = ht

		return nil
	}

	if eht.Equal(ht) {
		return util.ErrDuplicated.Errorf("hint, %q already added", ht)
	}

	switch {
	case !eht.Version().IsCompatible(ht.Version()):
		st.set[ht.Type()][ht.Version().Major()] = v
		st.hints[ht.Type()][ht.Version().Major()] = ht
	case ht.Version().Compare(eht.Version()) > 0:
		st.set[ht.Type()][ht.Version().Major()] = v
		st.hints[ht.Type()][ht.Version().Major()] = ht
	}

	return nil
}

func (st *CompatibleSet) Find(ht Hint) interface{} {
	switch i, err := st.cache.Get(ht.String()); {
	case err == nil:
		return i
	case !errors.Is(err, gcache.KeyNotFoundError):
		return nil
	}

	hr := st.find(ht)

	_ = st.cache.Set(ht.String(), hr)

	return hr
}

func (st *CompatibleSet) FindBytType(t Type) (ht Hint, value interface{}) {
	vs, found := st.typeheads[t]
	if !found {
		return ht, nil
	}

	return st.typeheadhints[t], vs
}

func (st *CompatibleSet) Traverse(f func(Hint, interface{}) bool) {
	for i := range st.set {
		for j := range st.set[i] {
			if !f(st.hints[i][j], st.set[i][j]) {
				return
			}
		}
	}
}

func (st *CompatibleSet) find(ht Hint) interface{} {
	vs, found := st.set[ht.Type()]
	if !found {
		return nil
	}

	v, found := vs[ht.Version().Major()]
	if !found {
		return nil
	}

	return v
}
