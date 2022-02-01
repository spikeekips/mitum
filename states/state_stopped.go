package states

type StoppedHandler struct {
	*baseStateHandler
}

func NewStoppedHandler() *StoppedHandler {
	return &StoppedHandler{
		baseStateHandler: newBaseStateHandler(StateStopped),
	}
}
