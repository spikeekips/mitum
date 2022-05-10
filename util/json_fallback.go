//go:build !amd64
// +build !amd64

package util

import jsoniter "github.com/json-iterator/go"

var jsoniterconfiged = jsoniter.Config{
	EscapeHTML: false,
}.Froze()

func marshalJSON(v interface{}) ([]byte, error) {
	return jsoniterconfiged.Marshal(v) //nolint:wrapcheck //...
}

func unmarshalJSON(b []byte, v interface{}) error {
	return jsoniterconfiged.Unmarshal(b, v) //nolint:wrapcheck //...
}

func marshalJSONIndent(i interface{}) ([]byte, error) {
	return jsoniterconfiged.MarshalIndent(i, "", "  ") //nolint:wrapcheck //...
}
