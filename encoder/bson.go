package encoder

import (
	"reflect"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/xerrors"

	"github.com/spikeekips/mitum/hint"
)

var bsonHint hint.Hint = hint.MustHint(hint.Type([2]byte{0x01, 0x02}), "0.1")

type BSONEncoder struct {
	cache   *cache
	hintset *hint.Hintset
}

func NewBSONEncoder() *BSONEncoder {
	return &BSONEncoder{
		cache: newCache(),
	}
}

func (be *BSONEncoder) SetHintset(hintset *hint.Hintset) {
	be.hintset = hintset
}

func (be BSONEncoder) Hint() hint.Hint {
	return bsonHint
}

func (be BSONEncoder) Marshal(i interface{}) ([]byte, error) {
	return bson.Marshal(i)
}

func (be BSONEncoder) Unmarshal(b []byte, i interface{}) error {
	return bson.Unmarshal(b, i)
}

func (be *BSONEncoder) Encode(i interface{}) ([]byte, error) {
	var target interface{} = i
	if i != nil {
		n, err := be.Pack(i)
		if err != nil {
			return nil, err
		}

		if n != nil {
			target = n
		}
	}

	return bson.Marshal(target)
}

func (be *BSONEncoder) Decode(b []byte, i interface{}) error {
	return be.Unpack(b, i)
}

func (be *BSONEncoder) DecodeByHint(b []byte) (hint.Hinter, error) {
	if be.hintset == nil {
		return nil, xerrors.Errorf("SetHintset() first")
	}

	h, err := be.loadHint(b)
	if err != nil {
		return nil, err
	}
	hinter, err := be.hintset.Hinter(h.Type(), h.Version())
	if err != nil {
		return nil, err
	}

	p := reflect.New(reflect.TypeOf(hinter))
	if err := be.Decode(b, p.Interface()); err != nil {
		return nil, err
	}

	return p.Elem().Interface().(hint.Hinter), nil
}

func (be *BSONEncoder) Analyze(i interface{}) error {
	_, elem := ExtractPtr(i)
	_, found := be.cache.Get(elem.Type())
	if found {
		return nil
	}

	_, cp, err := be.analyze(i)
	if err != nil {
		return err
	}

	be.cache.Set(cp.Type, cp)

	return nil
}

func (be *BSONEncoder) analyze(i interface{}) (string, CachedPacker, error) { // nolint
	name, pack, unpack := be.analyzeInstance(i)

	// hint
	_, elem := ExtractPtr(i)
	if elem.Kind() == reflect.Struct {
		var hinter hint.Hinter
		if ht, ok := elem.Interface().(hint.Hinter); ok {
			hinter = ht
		}

		pack = be.wrapPackerHinter(hinter, pack)
		unpack = be.wrapUnpackerHinter(hinter, unpack)
	}

	return name, NewCachedPacker(elem.Type(), pack, unpack), nil
}

func (be *BSONEncoder) analyzeInstance(i interface{}) (string, bsonPackFunc, bsonUnpackFunc) { // nolint
	var names []string
	var pf bsonPackFunc
	var upf bsonUnpackFunc

	ptr, elem := ExtractPtr(i)

	if _, ok := elem.Interface().(BSONPackable); ok {
		names = append(names, "BSONPackable")
		pf = func(i interface{}) (interface{}, error) {
			return i.(BSONPackable).PackBSON(be)
		}
	}

	if _, ok := ptr.Interface().(BSONUnpackable); ok {
		names = append(names, "BSONUnpackable")
		upf = func(b []byte, i interface{}) (interface{}, error) {
			if err := i.(BSONUnpackable).UnpackBSON(b, be); err != nil {
				return nil, err
			}

			return reflect.ValueOf(i).Elem().Interface(), nil
		}
	}

	if pf != nil || upf != nil {
		if pf == nil {
			pf = be.packValueDefault
		}
		if upf == nil {
			upf = be.unpackValueDefault
		}

		return strings.Join(names, "+"), pf, upf
	}

	pf = be.packValueDefault
	upf = be.unpackValueDefault

	return encoderAnalyzedTypeDefault, pf, upf
}

func (be *BSONEncoder) Pack(i interface{}) (interface{}, error) {
	_, elem := ExtractPtr(i)

	if c, found := be.cache.Get(elem.Type()); found {
		if packer, ok := c.(CachedPacker); !ok {
			be.cache.Delete(elem.Type())
		} else if fn, ok := packer.Pack.(bsonPackFunc); !ok {
			be.cache.Delete(elem.Type())
		} else {
			return fn(i)
		}
	}

	_, cp, err := be.analyze(i)
	if err != nil {
		return nil, err
	}

	be.cache.Set(cp.Type, cp)

	return cp.Pack.(bsonPackFunc)(i)
}

func (be *BSONEncoder) packValueDefault(i interface{}) (interface{}, error) {
	return i, nil
}

func (be *BSONEncoder) Unpack(b []byte, i interface{}) error {
	if n, err := be.unpackValue(b, i); err != nil {
		return err
	} else if n != nil {
		reflect.ValueOf(i).Elem().Set(reflect.ValueOf(n))
		return nil
	}

	return bson.Unmarshal(b, i)
}

func (be *BSONEncoder) unpackValue(b []byte, i interface{}) (interface{}, error) {
	_, elem := ExtractPtr(i)

	if c, found := be.cache.Get(elem.Type()); found {
		if packer, ok := c.(CachedPacker); !ok {
			be.cache.Delete(elem.Type())
		} else if fn, ok := packer.Unpack.(bsonUnpackFunc); !ok {
			be.cache.Delete(elem.Type())
		} else {
			return be.callUnpacker(b, i, fn)
		}
	}

	_, cp, err := be.analyze(i)
	if err != nil {
		return nil, err
	}

	be.cache.Set(cp.Type, cp)

	return be.callUnpacker(b, i, cp.Unpack.(bsonUnpackFunc))
}

func (be *BSONEncoder) callUnpacker(b []byte, i interface{}, fn bsonUnpackFunc) (interface{}, error) {
	n, err := fn(b, i)
	if err != nil {
		return nil, err
	} else if n != nil {
		return n, nil
	}

	if err := bson.Unmarshal(b, i); err != nil {
		return nil, err
	}

	return reflect.ValueOf(i).Elem().Interface(), nil
}

func (be *BSONEncoder) unpackValueDefault(b []byte, i interface{}) (interface{}, error) {
	if err := bson.Unmarshal(b, i); err != nil {
		return nil, err
	}

	return reflect.ValueOf(i).Elem().Interface(), nil
}

func (be BSONEncoder) wrapPackerHinter(hinter hint.Hinter, fn bsonPackFunc) bsonPackFunc {
	if hinter == nil {
		return fn
	}

	return func(i interface{}) (interface{}, error) {
		o, err := fn(i)
		if err != nil {
			return nil, err
		}

		return BSONPackHinted{H: hinter.Hint(), D: o}, nil
	}
}

func (be BSONEncoder) wrapUnpackerHinter(hinter hint.Hinter, fn bsonUnpackFunc) bsonUnpackFunc {
	if hinter == nil {
		return fn
	}

	return func(b []byte, i interface{}) (interface{}, error) {
		var o BSONUnpackHinted
		if err := bson.Unmarshal(b, &o); err != nil {
			return nil, err
		}

		if err := hinter.Hint().IsCompatible(o.H); err != nil {
			return nil, err
		}

		return fn(o.D, i)
	}
}

func (be BSONEncoder) loadHint(b []byte) (hint.Hint, error) {
	var o BSONUnpackHinted
	if err := bson.Unmarshal(b, &o); err != nil {
		return hint.Hint{}, err
	}

	return o.H, nil
}

type bsonPackFunc func(interface{}) (interface{}, error)
type bsonUnpackFunc func([]byte, interface{}) (interface{}, error)

type BSONPackable interface {
	PackBSON(*BSONEncoder) (interface{}, error)
}

type BSONUnpackable interface {
	UnpackBSON([]byte, *BSONEncoder) error
}

type BSONPackHinted struct {
	H hint.Hint   `bson:"_hint"`
	D interface{} `bson:"_data"`
}

type BSONUnpackHinted struct {
	H hint.Hint `bson:"_hint"`
	D bson.Raw  `bson:"_data,omitempty"`
}