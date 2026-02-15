package rabbitmq

// Return represents a message returned by the broker (unroutable)
type Return struct {
	ReplyCode  uint16
	ReplyText  string
	Exchange   string
	RoutingKey string
	Properties Properties
	Body       []byte
}

// ReturnListener handles returned messages
type ReturnListener interface {
	HandleReturn(ret Return)
}

// NotifyReturn registers a channel to receive returned messages
func (ch *Channel) NotifyReturn(returnChan chan Return) chan Return {
	ch.returnMux.Lock()
	defer ch.returnMux.Unlock()

	ch.returnChans = append(ch.returnChans, returnChan)
	return returnChan
}

// AddReturnListener adds a callback-based return listener
func (ch *Channel) AddReturnListener(listener ReturnListener) {
	ch.returnMux.Lock()
	defer ch.returnMux.Unlock()

	ch.returnListeners = append(ch.returnListeners, listener)
}
