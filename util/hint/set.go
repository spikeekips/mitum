package hint

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type CompatibleSet struct {
	set           map[Type]map[uint64]interface{}
	hints         map[Type]map[uint64]Hint
	typeheads     map[Type]interface{}
	typeheadhints map[Type]Hint
	cache         *util.GCache[string, any]
}

func NewCompatibleSet(size int) *CompatibleSet {
	var cache *util.GCache[string, any]
	if size > 0 {
		cache = util.NewLRUGCache[string, any](1)
	}

	return &CompatibleSet{
		set:           map[Type]map[uint64]interface{}{},
		hints:         map[Type]map[uint64]Hint{},
		typeheads:     map[Type]interface{}{},
		typeheadhints: map[Type]Hint{},
		cache:         cache,
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
		return errors.WithMessage(err, "add to CompatibleSet")
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
		return errors.WithMessage(err, "add with hint")
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
		return errors.Errorf("hint, %q already added", ht)
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
	if i, found, _ := st.cacheGet(ht.String()); found {
		return i
	}

	hr := st.find(ht)

	st.cacheSet(ht.String(), hr)

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

func (st *CompatibleSet) cacheGet(s string) (v interface{}, found bool, _ error) {
	if st.cache == nil {
		return nil, false, nil
	}

	switch i, found := st.cache.Get(s); {
	case !found:
		return nil, false, nil
	default:
		if e, ok := i.(error); ok {
			return nil, false, e
		}

		return i, true, nil
	}
}

func (st *CompatibleSet) cacheSet(s string, v interface{}) {
	if st.cache == nil {
		return
	}

	st.cache.Set(s, v, 0)
}
