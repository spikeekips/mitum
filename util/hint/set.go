package hint

import (
	"github.com/bluele/gcache"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type CompatibleSet struct {
	set   map[Type]map[uint64]interface{}
	hints map[Type]map[uint64]Hint
	cache gcache.Cache
}

func NewCompatibleSet() *CompatibleSet {
	return &CompatibleSet{
		set:   map[Type]map[uint64]interface{}{},
		hints: map[Type]map[uint64]Hint{},
		cache: gcache.New(100 * 100).LRU().Build(),
	}
}

func (st *CompatibleSet) Add(ht Hint, v interface{}) error {
	return st.add(ht, v)
}

func (st *CompatibleSet) AddHinter(hr Hinter) error {
	return st.add(hr.Hint(), hr)
}

func (st *CompatibleSet) add(ht Hint, v interface{}) error {
	if err := ht.IsValid(nil); err != nil {
		return errors.Wrapf(err, "failed to add to CompatibleSet")
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

	switch {
	case eht.Equal(ht):
		return util.FoundError.Errorf("hint, %q already added", ht)
	case !eht.Version().IsCompatible(ht.Version()):
		st.set[ht.Type()][ht.Version().Major()] = v
		st.hints[ht.Type()][ht.Version().Major()] = ht

		return nil
	case eht.Version().Compare(ht.Version()) > 0:
		st.set[ht.Type()][ht.Version().Major()] = v
		st.hints[ht.Type()][ht.Version().Major()] = ht

		return nil
	default:
		return nil
	}
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

type SafeTypeCompatibleSet struct {
	*CompatibleSet
}

func NewSafeTypeCompatibleSet() *SafeTypeCompatibleSet {
	return &SafeTypeCompatibleSet{
		CompatibleSet: NewCompatibleSet(),
	}
}

func (st *SafeTypeCompatibleSet) IsValid([]byte) error {
	for i := range st.set {
		if len(st.set[i]) < 1 {
			return errors.Errorf("some type,  %q is empty", i)
		}
	}

	return nil
}

func (st *SafeTypeCompatibleSet) AddType(t Type) error {
	if _, found := st.set[t]; found {
		return util.FoundError.Errorf("type already added to SafeTypeCompatibleSet, %q", t)
	}

	st.set[t] = map[uint64]interface{}{}
	st.hints[t] = map[uint64]Hint{}

	return nil
}

func (st *SafeTypeCompatibleSet) Add(ht Hint, v interface{}) error {
	if err := st.check(ht); err != nil {
		return err
	}

	return st.CompatibleSet.Add(ht, v)
}

func (st *SafeTypeCompatibleSet) AddHinter(hr Hinter) error {
	ht := hr.Hint()
	if err := st.check(ht); err != nil {
		return err
	}

	return st.CompatibleSet.AddHinter(hr)
}

func (st *SafeTypeCompatibleSet) check(ht Hint) error {
	if _, found := st.set[ht.Type()]; !found {
		return util.NotFoundError.Errorf("unknown type in SafeTypeCompatibleSet, %q", ht.Type())
	}

	return nil
}
