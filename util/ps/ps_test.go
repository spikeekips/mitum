package ps

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
)

type baseTest struct {
	suite.Suite
}

func (t *baseTest) emptyfunc() Func {
	return func(ctx context.Context) (context.Context, error) { return ctx, nil }
}

func (t *baseTest) calledfunc(name string) Func {
	key := ContextKey("called")

	return func(ctx context.Context) (context.Context, error) {
		var called []string
		if i := ctx.Value(key); i != nil {
			called = i.([]string)
		}

		called = append(called, name)

		return context.WithValue(ctx, key, called), nil
	}
}

type testHooks struct {
	baseTest
}

func (t *testHooks) TestAdd() {
	h := newHooks("killme")

	t.Run("add", func() {
		t.True(h.add("showme", t.emptyfunc()))
	})

	t.Run("add nil", func() {
		t.False(h.add("killme", nil))
	})

	t.Run("add again", func() {
		t.False(h.add("showme", t.emptyfunc()))
	})

	t.Run("found in names", func() {
		_, found := h.names["showme"]
		t.True(found)
	})

	t.Run("add other", func() {
		t.True(h.add("findme", t.emptyfunc()))
		_, found := h.names["findme"]
		t.True(found)

		t.Equal(2, len(h.l))
		t.Equal(2, len(h.names))

		t.Equal([]PName{"showme", "findme"}, h.l)
	})
}

func (t *testHooks) TestRemove() {
	h := newHooks("killme")

	t.Run("add 2", func() {
		t.True(h.add("showme", t.emptyfunc()))
		t.True(h.add("findme", t.emptyfunc()))
	})

	t.Run("remove 2nd", func() {
		t.True(h.remove("findme"))
	})

	t.Run("remove again", func() {
		t.False(h.remove("findme"))
		t.Equal(1, len(h.l))
		t.Equal(1, len(h.names))
		t.Equal([]PName{"showme"}, h.l)

		_, found := h.names["findme"]
		t.False(found)
	})

	t.Run("remove 1st", func() {
		t.True(h.remove("showme"))
		t.Equal(0, len(h.l))
		t.Equal(0, len(h.names))

		_, found := h.names["showme"]
		t.False(found)
	})
}

func (t *testHooks) TestBefore() {
	h := newHooks("killme")

	t.Run("unknown before", func() {
		t.False(h.before("findme", t.emptyfunc(), "showme"))
	})

	t.Run("before", func() {
		t.True(h.add("showme", t.emptyfunc()))
		t.True(h.before("findme", t.emptyfunc(), "showme"))

		t.Equal([]PName{"findme", "showme"}, h.l)
	})

	t.Run("before nil", func() {
		t.False(h.before("killme", nil, "showme"))
	})

	t.Run("before again", func() {
		t.False(h.before("findme", t.emptyfunc(), "showme"))

		t.Equal([]PName{"findme", "showme"}, h.l)
	})
}

func (t *testHooks) TestAfter() {
	h := newHooks("killme")

	t.Run("unknown after", func() {
		t.False(h.after("findme", t.emptyfunc(), "showme"))
	})

	t.Run("after", func() {
		t.True(h.add("showme", t.emptyfunc()))
		t.True(h.after("findme", t.emptyfunc(), "showme"))

		t.Equal([]PName{"showme", "findme"}, h.l)
	})

	t.Run("after nil", func() {
		t.False(h.after("killme", nil, "showme"))
	})

	t.Run("after again", func() {
		t.False(h.after("findme", t.emptyfunc(), "showme"))

		t.Equal([]PName{"showme", "findme"}, h.l)
	})
}

func (t *testHooks) TestRun() {
	h := newHooks("killme")

	t.True(h.add("a", t.calledfunc("a")))
	t.True(h.add("c", t.calledfunc("c")))
	t.True(h.before("b", t.calledfunc("b"), "c"))

	ctx := context.Background()

	rctx, err := h.run(ctx)
	t.NoError(err)

	rcalled := rctx.Value(ContextKey("called"))
	t.Equal([]string{"a", "b", "c"}, rcalled)
}

func TestHooks(t *testing.T) {
	suite.Run(t, new(testHooks))
}

type testP struct {
	baseTest
}

func (t *testP) TestAdd() {
	p := NewP(t.emptyfunc(), t.emptyfunc(), "a", "b", "c")
	t.Equal([]PName{"a", "b", "c"}, p.Requires())
}

func (t *testP) TestPreAdd() {
	p := NewP(nil, nil)

	t.Run("PreAdd", func() {
		t.True(p.PreAdd("showme", t.emptyfunc()))
	})

	t.Run("PreAdd again", func() {
		t.False(p.PreAdd("showme", t.emptyfunc()))
	})

	t.True(p.PreAdd("findme", t.emptyfunc()))

	t.Equal([]PName{"showme", "findme"}, p.pre.l)
}

func (t *testP) TestPreBefore() {
	p := NewP(nil, nil)

	t.True(p.PreAdd("findme", t.emptyfunc()))

	t.True(p.PreBefore("showme", t.emptyfunc(), "findme"))
	t.False(p.PreBefore("showme", t.emptyfunc(), "findme"))

	t.Equal([]PName{"showme", "findme"}, p.pre.l)
}

func (t *testP) TestPreAfter() {
	p := NewP(nil, nil)

	t.True(p.PreAdd("showme", t.emptyfunc()))

	t.True(p.PreAfter("findme", t.emptyfunc(), "showme"))
	t.False(p.PreAfter("findme", t.emptyfunc(), "showme"))

	t.Equal([]PName{"showme", "findme"}, p.pre.l)
}

func (t *testP) TestPreRemove() {
	p := NewP(nil, nil)

	t.True(p.PreAdd("showme", t.emptyfunc()))
	t.True(p.PreAdd("findme", t.emptyfunc()))

	t.True(p.PreRemove("findme"))
	t.False(p.PreRemove("findme"))
	t.Equal([]PName{"showme"}, p.pre.l)

	t.True(p.PreRemove("showme"))
	t.False(p.PreRemove("showme"))
	t.Equal([]PName{}, p.pre.l)
}

func (t *testP) TestPostAdd() {
	p := NewP(nil, nil)

	t.Run("PostAdd", func() {
		t.True(p.PostAdd("showme", t.emptyfunc()))
	})

	t.Run("PostAdd again", func() {
		t.False(p.PostAdd("showme", t.emptyfunc()))
	})

	t.True(p.PostAdd("findme", t.emptyfunc()))

	t.Equal([]PName{"showme", "findme"}, p.post.l)
}

func (t *testP) TestPostBefore() {
	p := NewP(nil, nil)

	t.True(p.PostAdd("findme", t.emptyfunc()))

	t.True(p.PostBefore("showme", t.emptyfunc(), "findme"))
	t.False(p.PostBefore("showme", t.emptyfunc(), "findme"))

	t.Equal([]PName{"showme", "findme"}, p.post.l)
}

func (t *testP) TestPostAfter() {
	p := NewP(nil, nil)

	t.True(p.PostAdd("showme", t.emptyfunc()))

	t.True(p.PostAfter("findme", t.emptyfunc(), "showme"))
	t.False(p.PostAfter("findme", t.emptyfunc(), "showme"))

	t.Equal([]PName{"showme", "findme"}, p.post.l)
}

func (t *testP) TestPostRemove() {
	p := NewP(nil, nil)

	t.True(p.PostAdd("showme", t.emptyfunc()))
	t.True(p.PostAdd("findme", t.emptyfunc()))

	t.True(p.PostRemove("findme"))
	t.False(p.PostRemove("findme"))
	t.Equal([]PName{"showme"}, p.post.l)

	t.True(p.PostRemove("showme"))
	t.False(p.PostRemove("showme"))
	t.Equal([]PName{}, p.post.l)
}

func (t *testP) TestRun() {
	p := NewP(t.calledfunc("run"), t.calledfunc("close"))

	ctx := context.Background()

	t.Run("run", func() {
		rctx, err := p.Run(ctx)
		t.NoError(err)

		rcalled := rctx.Value(ContextKey("called"))
		t.Equal([]string{"run"}, rcalled)

		ctx = rctx
	})

	t.Run("close", func() {
		cctx, err := p.Close(ctx)
		t.NoError(err)

		ccalled := cctx.Value(ContextKey("called"))
		t.Equal([]string{"run", "close"}, ccalled)
	})
}

func (t *testP) TestRunPrePost() {
	p := NewP(t.calledfunc("run"), t.calledfunc("close"))

	t.True(p.PreAdd("pre-a", t.calledfunc("pre-a")))
	t.True(p.PreAdd("pre-b", t.calledfunc("pre-b")))
	t.True(p.PostAdd("post-a", t.calledfunc("post-a")))
	t.True(p.PostAdd("post-b", t.calledfunc("post-b")))

	ctx := context.Background()

	t.Run("run", func() {
		rctx, err := p.Run(ctx)
		t.NoError(err)

		rcalled := rctx.Value(ContextKey("called"))
		t.Equal([]string{"pre-a", "pre-b", "run", "post-a", "post-b"}, rcalled)

		ctx = rctx
	})

	t.Run("close", func() {
		cctx, err := p.Close(ctx)
		t.NoError(err)

		ccalled := cctx.Value(ContextKey("called"))
		t.Equal([]string{"pre-a", "pre-b", "run", "post-a", "post-b", "close"}, ccalled)
	})
}

func TestP(t *testing.T) {
	suite.Run(t, new(testP))
}

type testPS struct {
	baseTest
}

func (t *testPS) TestNew() {
	ps := NewPS()

	t.Run("P; not found", func() {
		p, found := ps.P("showme")
		t.Nil(p)
		t.False(found)
	})

	t.Run("P; found", func() {
		t.True(ps.Add("showme", nil, nil))

		p, found := ps.P("showme")
		t.NotNil(p)
		t.True(found)
	})
}

func (t *testPS) TestAdd() {
	ps := NewPS()

	t.Run("add", func() {
		t.True(ps.Add("showme", nil, nil))
	})

	t.Run("add again", func() {
		t.False(ps.Add("showme", nil, nil))
	})
}

func (t *testPS) TestRemove() {
	ps := NewPS()

	t.True(ps.Add("showme", nil, nil))

	t.Run("remove", func() {
		t.True(ps.Remove("showme"))
	})

	t.Run("remove again", func() {
		t.False(ps.Remove("showme"))
	})
}

func (t *testPS) TestRun() {
	ps := NewPS()

	t.True(ps.Add("c", t.calledfunc("c-run"), t.calledfunc("c-close")))
	t.True(ps.Add("b", t.calledfunc("b-run"), t.calledfunc("b-close")))
	t.True(ps.Add("a", t.calledfunc("a-run"), t.calledfunc("a-close")))

	ctx := context.Background()

	t.Run("run", func() {
		rctx, err := ps.Run(ctx)
		t.NoError(err)

		rcalled := rctx.Value(ContextKey("called"))
		t.Equal([]string{"a-run", "b-run", "c-run"}, rcalled)
	})

	t.Run("run with error", func() {
		ps.Reset()

		t.False(ps.Add(
			"b",
			func(ctx context.Context) (context.Context, error) {
				return ctx, errors.Errorf("hihihi")
			},
			t.calledfunc("b-close"),
		))

		rctx, err := ps.Run(ctx)
		t.Error(err)
		t.ErrorContains(err, "hihihi")

		rcalled := rctx.Value(ContextKey("called"))
		t.Equal([]string{"a-run"}, rcalled)
		t.Equal([]PName{"a", "b"}, ps.runs)

		rctx, err = ps.Close(rctx)
		t.NoError(err)

		rcalled = rctx.Value(ContextKey("called"))
		t.Equal([]string{"a-run", "b-close", "a-close"}, rcalled)
	})

	t.Run("close", func() {
		ps.Reset()

		t.False(ps.Add("b", t.calledfunc("b-run"), t.calledfunc("b-close")))

		rctx, err := ps.Run(ctx)
		t.NoError(err)

		rctx, err = ps.Close(rctx)
		t.NoError(err)

		rcalled := rctx.Value(ContextKey("called"))
		t.Equal([]string{"a-run", "b-run", "c-run", "c-close", "b-close", "a-close"}, rcalled)
	})
}

func (t *testPS) TestVerbose() {
	t.Run("no pre post", func() {
		ps := NewPS()

		t.True(ps.Add("c", t.calledfunc("c-run"), nil))
		t.True(ps.Add("b", t.calledfunc("b-run"), nil))
		t.True(ps.Add("a", t.calledfunc("a-run"), nil))

		names := ps.Verbose()
		t.Equal([]PName{"a:run", "b:run", "c:run"}, names)
	})

	t.Run("pre post", func() {
		ps := NewPS()

		t.True(ps.Add("a", t.calledfunc("a-run"), nil))
		t.True(ps.Add("b", t.calledfunc("b-run"), nil))
		t.True(ps.Add("c", t.calledfunc("c-run"), nil))

		a, _ := ps.P("a")
		a.PreAdd("aa", t.emptyfunc())
		a.PreAdd("ab", t.emptyfunc())
		a.PostAdd("ac", t.emptyfunc())

		b, _ := ps.P("b")
		b.PreAdd("ba", t.emptyfunc())
		b.PreAdd("bb", t.emptyfunc())
		b.PostAdd("bc", t.emptyfunc())

		c, _ := ps.P("c")
		c.PreAdd("ca", t.emptyfunc())
		c.PreAdd("cb", t.emptyfunc())
		c.PostAdd("cc", t.emptyfunc())

		names := ps.Verbose()
		t.Equal([]PName{"aa:pre", "ab:pre", "a:run", "ac:post", "ba:pre", "bb:pre", "b:run", "bc:post", "ca:pre", "cb:pre", "c:run", "cc:post"}, names)
	})
}

func TestPS(t *testing.T) {
	suite.Run(t, new(testPS))
}
