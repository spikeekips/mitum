package jsonenc

type Decodable interface {
	DecodeJSON([]byte, *Encoder) error
}
