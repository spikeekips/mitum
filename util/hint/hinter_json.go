package hint

const HintedJSONTag = "_hint"

type HintedJSONHead struct {
	H Hint `json:"_hint"`
}

func NewHintedJSONHead(h Hint) HintedJSONHead {
	return HintedJSONHead{H: h}
}
