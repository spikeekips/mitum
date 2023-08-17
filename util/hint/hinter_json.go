package hint

const HintedJSONTag = "_hint"

type HinterJSONHead struct {
	H Hint `json:"_hint"` //nolint:tagliatelle //...
}

func NewHinterJSONHead(h Hint) HinterJSONHead {
	return HinterJSONHead{H: h}
}

type HintedJSONHead struct {
	H string `json:"_hint"` //nolint:tagliatelle //...
}
