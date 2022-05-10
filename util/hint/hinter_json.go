package hint

const HintedJSONTag = "_hint"

type HintedJSONHead struct {
	H Hint `json:"_hint"` //nolint:tagliatelle //...
}

func NewHintedJSONHead(h Hint) HintedJSONHead {
	return HintedJSONHead{H: h}
}
