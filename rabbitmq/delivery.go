package rabbitmq

// Delivery represents a message delivered to a consumer
type Delivery struct {
	// Message metadata
	ConsumerTag string
	DeliveryTag uint64
	Redelivered bool
	Exchange    string
	RoutingKey  string

	// Message content
	Properties Properties
	Body       []byte

	// Channel reference for acknowledgment
	channel *Channel
}

// Ack acknowledges this delivery
func (d *Delivery) Ack(multiple bool) error {
	if d.channel == nil {
		return ErrChannelClosed
	}
	return d.channel.BasicAck(d.DeliveryTag, multiple)
}

// Nack negatively acknowledges this delivery
func (d *Delivery) Nack(multiple, requeue bool) error {
	if d.channel == nil {
		return ErrChannelClosed
	}
	return d.channel.BasicNack(d.DeliveryTag, multiple, requeue)
}

// Reject rejects this delivery
func (d *Delivery) Reject(requeue bool) error {
	if d.channel == nil {
		return ErrChannelClosed
	}
	return d.channel.BasicReject(d.DeliveryTag, requeue)
}

// GetResponse represents a response from BasicGet (polled message)
type GetResponse struct {
	// Message metadata
	DeliveryTag  uint64
	Redelivered  bool
	Exchange     string
	RoutingKey   string
	MessageCount int // Number of messages remaining in queue

	// Message content
	Properties Properties
	Body       []byte

	// Channel reference for acknowledgment
	channel *Channel
}

// Ack acknowledges this message
func (gr *GetResponse) Ack(multiple bool) error {
	if gr.channel == nil {
		return ErrChannelClosed
	}
	return gr.channel.BasicAck(gr.DeliveryTag, multiple)
}

// Nack negatively acknowledges this message
func (gr *GetResponse) Nack(multiple, requeue bool) error {
	if gr.channel == nil {
		return ErrChannelClosed
	}
	return gr.channel.BasicNack(gr.DeliveryTag, multiple, requeue)
}

// Reject rejects this message
func (gr *GetResponse) Reject(requeue bool) error {
	if gr.channel == nil {
		return ErrChannelClosed
	}
	return gr.channel.BasicReject(gr.DeliveryTag, requeue)
}

// Queue represents queue information returned from QueueDeclare
type Queue struct {
	Name      string
	Messages  int
	Consumers int
}
