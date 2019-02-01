package rabbit

import (
	"encoding/json"

	"github.com/babex-group/babex"
	"github.com/streadway/amqp"
)

type Converter func(msg *amqp.Delivery) (*babex.Message, error)

func NewMessage(msg *amqp.Delivery) (*babex.Message, error) {
	var initialMessage babex.InitialMessage

	if err := json.Unmarshal(msg.Body, &initialMessage); err != nil {
		return nil, err
	}

	message := babex.NewMessage(&initialMessage, msg.Exchange, msg.RoutingKey)
	message.RawMessage = Message{msg: msg}

	return message, nil
}

type Message struct {
	msg *amqp.Delivery
}

func (m Message) Ack() error {
	return m.msg.Ack(false)
}

func (m Message) Nack() error {
	return m.msg.Nack(false, true)
}
