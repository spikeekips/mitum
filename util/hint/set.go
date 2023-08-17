package hint

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type CompatibleSet[T any] struct {
	set           map[Type]map[uint64]T
	hints         map[Type]map[uint64]Hint
	typeheads     map[Type]T
	typeheadhints map[Type]Hint
	cache         *util.GCache[string, any]
}

func NewCompatibleSet[T any](size int) *CompatibleSet[T] {
	var cache *util.GCache[string, any]
	if size > 0 {
		cache = util.NewLRUGCache[string, any](1)
	}

	return &CompatibleSet[T]{
		set:           map[Type]map[uint64]T{},
		hints:         map[Type]map[uint64]Hint{},
		typeheads:     map[Type]T{},
		typeheadhints: map[Type]Hint{},
		cache:         cache,
	}
}

func (st *CompatibleSet[T]) Add(ht Hint, v T) error {
	return st.add(ht, v)
}

func (st *CompatibleSet[T]) AddHinter(h Hinter) error {
	i, ok := h.(T)
	if !ok {
		var t T

		return errors.Errorf("expected %T, but %T", t, h)
	}

	return st.add(h.Hint(), i)
}

func (st *CompatibleSet[T]) add(ht Hint, v T) error {
	if err := st.addWithHint(ht, v); err != nil {
		return errors.WithMessage(err, "add to CompatibleSet")
	}

	st.cacheSet(ht.String(), [2]interface{}{ht, v})

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

func (st *CompatibleSet[T]) addWithHint(ht Hint, v T) error {
	if err := ht.IsValid(nil); err != nil {
		return errors.WithMessage(err, "add with hint")
	}

	if _, found := st.set[ht.Type()]; !found {
		st.set[ht.Type()] = map[uint64]T{ht.Version().Major(): v}
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

func (st *CompatibleSet[T]) Find(ht Hint) (v T, found bool) {
	switch _, i, found, foundincache, err := st.cacheGet(ht.String()); {
	case err != nil:
		return v, false
	case foundincache:
		return i, found
	}

	return st.find(ht)
}

func (st *CompatibleSet[T]) FindByString(s string) (ht Hint, v T, found bool, _ error) {
	switch i, j, cfound, foundincache, err := st.cacheGet(s); {
	case err != nil:
		return ht, v, false, err
	case foundincache:
		return i, j, cfound, nil
	}

	switch h, err := ParseHint(s); {
	case err != nil:
		st.cacheSet(s, err)

		return ht, v, false, err
	default:
		v, found = st.find(h)

		return h, v, found, nil
	}
}

func (st *CompatibleSet[T]) FindBytType(t Type) (ht Hint, v T, found bool) {
	switch ht, i, found, foundincache, err := st.cacheGet(t.String()); {
	case err != nil:
		return ht, v, false
	case foundincache:
		return ht, i, found
	}

	return st.findBytType(t)
}

func (st *CompatibleSet[T]) FindBytTypeString(s string) (ht Hint, v T, found bool, _ error) {
	switch i, j, cfound, foundincache, err := st.cacheGet(s); {
	case err != nil:
		return ht, v, false, err
	case foundincache:
		return i, j, cfound, nil
	}

	t := Type(s)
	if err := t.IsValid(nil); err != nil {
		st.cacheSet(s, err)

		return ht, v, false, err
	}

	ht, v, found = st.findBytType(t)

	return ht, v, found, nil
}

func (st *CompatibleSet[T]) Traverse(f func(Hint, T) bool) {
	for i := range st.set {
		for j := range st.set[i] {
			if !f(st.hints[i][j], st.set[i][j]) {
				return
			}
		}
	}
}

func (st *CompatibleSet[T]) find(ht Hint) (v T, found bool) {
	vs, found := st.set[ht.Type()]
	if !found {
		st.cacheSet(ht.String(), false)

		return v, false
	}

	v, found = vs[ht.Version().Major()]

	switch {
	case !found:
		st.cacheSet(ht.String(), false)
	default:
		st.cacheSet(ht.String(), [2]interface{}{ht, v})
	}

	return v, found
}

func (st *CompatibleSet[T]) findBytType(t Type) (ht Hint, v T, found bool) {
	vs, found := st.typeheads[t]
	if !found {
		st.cacheSet(t.String(), false)

		return ht, v, false
	}

	ht = st.typeheadhints[t]
	v = vs

	st.cacheSet(t.String(), [2]interface{}{ht, v})

	return ht, v, true
}

func (st *CompatibleSet[T]) cacheGet(s string) (ht Hint, v T, found, foundincache bool, err error) {
	if st.cache == nil {
		return ht, v, false, false, nil
	}

	switch i, cfound := st.cache.Get(s); {
	case !cfound:
		return ht, v, false, false, nil
	default:
		switch t := i.(type) {
		case error:
			err = t
		case bool:
			found = t
		default:
			found = true

			if j, ok := i.([2]interface{}); ok {
				ht = j[0].(Hint) //nolint:forcetypeassert //...
				v = j[1].(T)     //nolint:forcetypeassert //...
			}
		}

		return ht, v, found, true, err
	}
}

func (st *CompatibleSet[T]) cacheSet(s string, v interface{}) {
	if st.cache == nil {
		return
	}

	st.cache.Set(s, v, 0)
}
