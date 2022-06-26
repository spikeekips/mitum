//go:build !amd64
// +build !amd64

package util

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

var jsoniterconfiged = jsoniter.Config{
	EscapeHTML: false,
}.Froze()

var indentjsoniterconfiged = jsoniter.Config{
	IndentionStep: 2, //nolint:gomnd //...
	EscapeHTML:    false,
}.Froze()

func marshalJSON(v interface{}) ([]byte, error) {
	b, err := jsoniterconfiged.Marshal(v) //nolint:wrapcheck //...

	return b, errors.WithStack(err)
}

func unmarshalJSON(b []byte, v interface{}) error {
	err := jsoniterconfiged.Unmarshal(b, v) //nolint:wrapcheck //...

	return errors.WithStack(err)
}

func marshalJSONIndent(i interface{}) ([]byte, error) {
	b, err := indentjsoniterconfiged.MarshalIndent(i, "", "  ") //nolint:wrapcheck //...

	return b, errors.WithStack(err)
}
