package ps

import (
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var (
	NameINIT      = Name("init")
	EmptyPINIT    = NewP(EmptyFunc, EmptyFunc)
	EmptyFunc     = func(ctx context.Context) (context.Context, error) { return ctx, nil }
	ErrIgnoreLeft = util.NewError("ignore left")
)

type ContextKey string

type Func func(context.Context) (context.Context, error)

type Name string

func (p Name) String() string {
	return string(p)
}

type hooks struct {
	*logging.Logging
	names map[Name]Func
	l     []Name
}

func newHooks(name string) *hooks {
	return &hooks{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", name)
		}),
		names: map[Name]Func{},
	}
}

func (h *hooks) Verbose() []Name {
	return h.l
}

func (h *hooks) run(ctx context.Context) (context.Context, error) {
	if len(h.l) < 1 {
		return ctx, nil
	}

	var err error

	for i := range h.l {
		name := h.l[i]

		l := h.Log().With().Stringer("name", name).Logger()

		l.Debug().Msg("start to run hook")

		if ctx, err = h.names[name](ctx); err != nil { //revive:disable-line:modifies-parameter
			l.Error().Err(err).Msg("failed to run hook")

			return ctx, err
		}

		l.Debug().Msg("finished to run hook")
	}

	h.Log().Debug().Msg("finished to run hooks")

	return ctx, nil
}

func (h *hooks) add(name Name, i Func) bool {
	if i == nil {
		return false
	}

	if _, found := h.names[name]; found {
		return false
	}

	h.names[name] = i

	h.l = append(h.l, name)

	return true
}

func (h *hooks) before(name Name, i Func, before Name) bool {
	if i == nil {
		return false
	}

	if _, found := h.names[name]; found {
		return false
	}

	if _, found := h.names[before]; !found {
		return false
	}

	h.names[name] = i

	hooks := make([]Name, len(h.l)+1)

	var j int

	for j = range h.l {
		if h.l[j] == before {
			break
		}
	}

	copy(hooks, h.l[:j])
	hooks[j] = name
	copy(hooks[j+1:], h.l[j:])

	h.l = hooks

	return true
}

func (h *hooks) after(name Name, i Func, after Name) bool {
	if i == nil {
		return false
	}

	if _, found := h.names[name]; found {
		return false
	}

	if _, found := h.names[after]; !found {
		return false
	}

	h.names[name] = i

	hooks := make([]Name, len(h.l)+1)

	var j int
	for j = range h.l {
		if h.l[j] == after {
			break
		}
	}

	copy(hooks, h.l[:j+1])
	hooks[j+1] = name

	if j < len(h.l)-1 {
		copy(hooks[j+2:], h.l[j+1:])
	}

	h.l = hooks

	return true
}

func (h *hooks) remove(name Name) bool {
	if _, found := h.names[name]; !found {
		return false
	}

	delete(h.names, name)

	hooks := make([]Name, len(h.l)-1)

	var i int

	_ = util.FilterSlice(h.l, func(_ interface{}, j int) bool {
		if name == h.l[j] {
			return false
		}

		hooks[i] = h.l[j]
		i++

		return true
	})

	h.l = hooks

	return true
}

type P struct {
	*logging.Logging
	run      Func
	closef   Func
	pre      *hooks
	post     *hooks
	requires []Name
}

func NewP(run, closef Func, requires ...Name) *P {
	id := util.UUID()

	return &P{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "ps-process").Stringer("id", id)
		}),
		run:      run,
		closef:   closef,
		requires: requires,
		pre:      newHooks(id.String() + ":pre"),
		post:     newHooks(id.String() + ":post"),
	}
}

func (p *P) SetLogging(l *logging.Logging) *logging.Logging {
	_ = p.pre.SetLogging(l)
	_ = p.post.SetLogging(l)

	return p.Logging.SetLogging(l)
}

func (p *P) Requires() []Name {
	return p.requires
}

func (p *P) Verbose(name Name) []Name {
	names := make([]Name, len(p.pre.Verbose())+len(p.post.Verbose())+1)

	pre := p.pre.Verbose()

	for i := range pre {
		names[i] = pre[i] + ":pre"
	}

	names[len(pre)] = name + ":run"

	post := p.post.Verbose()

	for i := range post {
		names[i+len(pre)+1] = post[i] + ":post"
	}

	return names
}

func (p *P) Run(ctx context.Context) (context.Context, error) {
	p.Log().Debug().Msg("start to run")

	var err error

	if ctx, err = p.pre.run(ctx); err != nil { //revive:disable-line:modifies-parameter
		return ctx, err
	}

	if p.run != nil {
		if ctx, err = p.run(ctx); err != nil { //revive:disable-line:modifies-parameter
			return ctx, err
		}
	}

	if ctx, err = p.post.run(ctx); err != nil { //revive:disable-line:modifies-parameter
		return ctx, err
	}

	p.Log().Debug().Msg("finished to run")

	return ctx, nil
}

func (p *P) Close(ctx context.Context) (context.Context, error) {
	if p.closef == nil {
		return ctx, nil
	}

	p.Log().Debug().Msg("start to close")

	var err error

	ctx, err = p.closef(ctx) //revive:disable-line:modifies-parameter
	if err != nil {
		p.Log().Error().Err(err).Msg("failed to close")

		return ctx, err
	}

	p.Log().Debug().Msg("finished to run")

	return ctx, nil
}

func (p *P) CopyHooks(b *P) {
	p.pre = b.pre
	p.post = b.post
}

func (p *P) PreAdd(name Name, i Func) bool {
	return p.pre.add(name, i)
}

func (p *P) PreBefore(name Name, i Func, before Name) bool {
	return p.pre.before(name, i, before)
}

func (p *P) PreAfter(name Name, i Func, after Name) bool {
	return p.pre.after(name, i, after)
}

func (p *P) PreRemove(name Name) bool {
	return p.pre.remove(name)
}

func (p *P) PostAdd(name Name, i Func) bool {
	return p.post.add(name, i)
}

func (p *P) PostBefore(name Name, i Func, before Name) bool {
	return p.post.before(name, i, before)
}

func (p *P) PostAfter(name Name, i Func, after Name) bool {
	return p.post.after(name, i, after)
}

func (p *P) PostRemove(name Name) bool {
	return p.post.remove(name)
}

func (p *P) PreAddOK(name Name, i Func) *P {
	_ = p.PreAdd(name, i)

	return p
}

func (p *P) PreBeforeOK(name Name, i Func, before Name) *P {
	_ = p.PreBefore(name, i, before)

	return p
}

func (p *P) PreAfterOK(name Name, i Func, after Name) *P {
	_ = p.PreAfter(name, i, after)

	return p
}

func (p *P) PreRemoveOK(name Name) *P {
	_ = p.PreRemove(name)

	return p
}

func (p *P) PostAddOK(name Name, i Func) *P {
	_ = p.PostAdd(name, i)

	return p
}

func (p *P) PostBeforeOK(name Name, i Func, before Name) *P {
	_ = p.PostBefore(name, i, before)

	return p
}

func (p *P) PostAfterOK(name Name, i Func, after Name) *P {
	_ = p.PostAfter(name, i, after)

	return p
}

func (p *P) PostRemoveOK(name Name) *P {
	_ = p.PostRemove(name)

	return p
}

type PS struct {
	*logging.Logging
	m    map[Name]*P
	runs []Name
	sync.Mutex
}

func NewPS(name string) *PS {
	return &PS{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "ps-"+name)
		}),
		m: map[Name]*P{NameINIT: EmptyPINIT},
	}
}

func (ps *PS) SetLogging(l *logging.Logging) *logging.Logging {
	for k := range ps.m {
		_ = ps.m[k].SetLogging(l)
	}

	return ps.Logging.SetLogging(l)
}

func (ps *PS) Verbose() []Name {
	called := map[Name]struct{}{}

	var f func(name Name) []Name

	f = func(name Name) []Name {
		if _, found := called[name]; found {
			return nil
		}

		var names []Name

		p, found := ps.m[name]
		if !found {
			panic(errors.Errorf("p not found, %q", name))
		}

		for i := range p.requires {
			rnames := f(p.requires[i])

			nm := make([]Name, len(names)+len(rnames))
			copy(nm, names)
			copy(nm[len(names):], rnames)

			names = nm
		}

		cnames := p.Verbose(name)

		nm := make([]Name, len(names)+len(cnames))
		copy(nm, names)
		copy(nm[len(names):], cnames)

		names = nm

		called[name] = struct{}{}

		return names
	}

	psnames := ps.names()

	var names []Name

	for i := range psnames {
		cnames := f(psnames[i])

		nm := make([]Name, len(names)+len(cnames))
		copy(nm, names)
		copy(nm[len(names):], cnames)

		names = nm
	}

	return names
}

func (ps *PS) CloseVerbose() []Name {
	names := make([]Name, len(ps.runs))

	var i int

	for j := range ps.runs {
		name := ps.runs[len(ps.runs)-j-1]
		if ps.m[name].closef == nil {
			continue
		}

		names[i] = name
		i++
	}

	return names[:i]
}

func (ps *PS) POK(name Name) *P {
	return ps.m[name]
}

func (ps *PS) P(name Name) (*P, bool) {
	i, found := ps.m[name]

	return i, found
}

func (ps *PS) Run(ctx context.Context) (context.Context, error) {
	ps.Lock()
	defer ps.Unlock()

	ps.runs = nil

	ps.Log().Debug().Msg("start to run")

	names := ps.names()

	var err error

	for i := range names {
		name := names[i]

		switch ctx, err = ps.run(ctx, name, ps.m[name]); { //revive:disable-line:modifies-parameter
		case err == nil:
		case errors.Is(err, ErrIgnoreLeft):
			return ctx, nil
		default:
			return ctx, err
		}
	}

	ps.Log().Debug().Msg("finished to run")

	return ctx, nil
}

func (ps *PS) Close(ctx context.Context) (context.Context, error) {
	ps.Log().Debug().Msg("start to close")

	var err error

	for i := range ps.runs {
		name := ps.runs[len(ps.runs)-i-1]

		l := ps.Log().With().Interface("name", name).Logger()
		l.Debug().Msg("start to close process")

		if ctx, err = ps.m[name].Close(ctx); err != nil { //revive:disable-line:modifies-parameter
			l.Error().Err(err).Msg("failed to close process")

			return ctx, err
		}

		l.Debug().Msg("finished to close process")
	}

	ps.Log().Debug().Msg("finished to close")

	return ctx, nil
}

func (ps *PS) AddP(name Name, p *P) bool {
	_, exists := ps.m[name]

	ps.m[name] = p
	_ = p.SetLogging(ps.Logging)

	return !exists
}

func (ps *PS) Add(name Name, run, close Func, requires ...Name) bool {
	_, added := ps.add(name, run, close, requires...)

	return added
}

func (ps *PS) Replace(name Name, run, close Func, requires ...Name) bool {
	exists, found := ps.m[name]

	p, added := ps.add(name, run, close, requires...)

	if found && exists != nil {
		p.CopyHooks(exists)
	}

	return added
}

func (ps *PS) add(name Name, run, close Func, requires ...Name) (*P, bool) {
	if name != NameINIT {
		switch {
		case len(requires) < 1:
			requires = []Name{NameINIT} //revive:disable-line:modifies-parameter
		case util.InSliceFunc(requires, func(_ interface{}, i int) bool { return requires[i] == NameINIT }) < 1:
			n := make([]Name, len(requires)+1)
			n[0] = NameINIT
			copy(n[1:], requires)

			requires = n //revive:disable-line:modifies-parameter
		}
	}

	p := NewP(run, close, requires...)
	_ = p.SetLogging(ps.Logging)

	_, found := ps.m[name]

	ps.m[name] = p

	return p, !found
}

func (ps *PS) Remove(name Name) bool {
	if _, found := ps.m[name]; !found {
		return false
	}

	delete(ps.m, name)

	return true
}

func (ps *PS) ReplaceOK(name Name, run, close Func, requires ...Name) *PS {
	_ = ps.Replace(name, run, close, requires...)

	return ps
}

func (ps *PS) AddOK(name Name, run, close Func, requires ...Name) *PS {
	_ = ps.Add(name, run, close, requires...)

	return ps
}

func (ps *PS) RemoveOK(name Name) *PS {
	_ = ps.Remove(name)

	return ps
}

func (ps *PS) names() []Name {
	names := make([]Name, len(ps.m))

	var i int

	for k := range ps.m {
		names[i] = k
		i++
	}

	sort.Slice(names, func(i, j int) bool {
		return strings.Compare(string(names[i]), string(names[j])) < 0
	})

	return names
}

func (ps *PS) run(ctx context.Context, name Name, p *P) (context.Context, error) {
	for i := range ps.runs {
		if ps.runs[i] == name {
			return ctx, nil
		}
	}

	defer func() {
		ps.runs = append(ps.runs, name)
	}()

	if p == nil {
		return ctx, nil
	}

	var err error

	for i := range p.requires {
		rname := p.requires[i]
		rp, found := ps.m[rname]

		if !found {
			return ctx, util.ErrNotFound.Errorf("required process not found, %q", rname)
		}

		if ctx, err = ps.run(ctx, rname, rp); err != nil { //revive:disable-line:modifies-parameter
			return ctx, err
		}
	}

	l := ps.Log().With().Interface("name", name).Logger()
	l.Debug().Interface("requires", p.Requires()).Msg("start to run")

	if ctx, err = p.Run(ctx); err != nil { //revive:disable-line:modifies-parameter
		if !errors.Is(err, ErrIgnoreLeft) {
			l.Error().Err(err).Msg("failed to run")
		}

		return ctx, err
	}

	l.Debug().Msg("finished to run")

	return ctx, nil
}

func LoadFromContextOK(ctx context.Context, a ...interface{}) error {
	if err := checkLoadFromContext(ctx, a...); err != nil {
		return err
	}

	return loadFromContext(ctx, load, a...)
}

func LoadFromContext(ctx context.Context, a ...interface{}) error {
	switch err := checkLoadFromContext(ctx, a...); {
	case err == nil:
	case errors.Is(err, util.ErrNotFound):
	default:
		return err
	}

	return loadFromContext(ctx, load, a...)
}

func checkLoadFromContext(ctx context.Context, a ...interface{}) error {
	switch {
	case len(a) < 1:
		return nil
	case len(a)%2 != 0:
		return errors.Errorf("should be, [key value] pairs")
	}

	for i := 0; i < len(a)/2; i++ {
		b := a[i*2]

		k, ok := b.(ContextKey)
		if !ok {
			return errors.Errorf("expected ContextKey, not %T", b)
		}

		if ctx.Value(k) == nil {
			return util.ErrNotFound.Errorf("key not found, %q", k)
		}
	}

	return nil
}

func loadFromContext(
	ctx context.Context,
	load func(context.Context, ContextKey, interface{}) error,
	a ...interface{},
) error {
	for i := 0; i < len(a)/2; i++ {
		b := a[i*2]

		v := a[i*2+1]

		if err := load(ctx, b.(ContextKey), v); err != nil { //nolint:forcetypeassert //...
			return err
		}
	}

	return nil
}

func load(ctx context.Context, key ContextKey, v interface{}) error {
	i := ctx.Value(key)
	if i == nil {
		return nil
	}

	if err := util.InterfaceSetValue(i, v); err != nil {
		return errors.WithMessagef(err, "failed to load value from context, %q", key)
	}

	return nil
}
