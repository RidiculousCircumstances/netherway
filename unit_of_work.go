package netherway

import (
	"github.com/rabbitmq/amqp091-go"
)

type AMQPUnitOfWork struct {
	envelope *amqp091.Delivery
}

func NewAMQPUnitOfWork(envelope *amqp091.Delivery) *AMQPUnitOfWork {
	return &AMQPUnitOfWork{
		envelope: envelope,
	}
}

func (u *AMQPUnitOfWork) Ack() error {
	return u.envelope.Ack(false)
}

func (u *AMQPUnitOfWork) Nack(requeue bool) error {
	return u.envelope.Nack(false, requeue)
}

func (u *AMQPUnitOfWork) GetPayload() []byte {
	return u.envelope.Body
}
