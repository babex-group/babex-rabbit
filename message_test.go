package rabbit

import (
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewMessage(t *testing.T) {
	delivery := amqp.Delivery{
		Exchange:   "test",
		RoutingKey: "TestExample",
		Body:       []byte(`{"chain": [{"exchange": "hey"}]}`),
	}

	msg, err := NewMessage(&delivery)

	assert.Nil(t, err)
	assert.Equal(t, "test", msg.Exchange)
	assert.Equal(t, "TestExample", msg.Key)
	assert.Len(t, msg.Chain, 1)
	assert.Equal(t, "hey", msg.Chain[0].Exchange)
}
