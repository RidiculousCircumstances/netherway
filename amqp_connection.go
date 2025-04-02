package netherway

import (
	"context"
	"github.com/rabbitmq/amqp091-go"
)

type Exchange struct {
	Name string
	Type string
}

type amqpConnection struct {
	connection    *amqp091.Connection
	publisherPool *ChannelPool
	consumerPool  *ChannelPool
	exchanges     []Exchange
}

// Новый конструктор с поддержкой инициализации обменников
func NewAMQPConnection(
	conn *amqp091.Connection,
	publisherPoolSize, consumerPoolSize int,
	exchanges []Exchange,
) (Connection, error) {
	publisherPool, err := NewChannelPool(conn, publisherPoolSize)
	if err != nil {
		return nil, err
	}
	consumerPool, err := NewChannelPool(conn, consumerPoolSize)
	if err != nil {
		return nil, err
	}

	amqpConn := &amqpConnection{
		connection:    conn,
		publisherPool: publisherPool,
		consumerPool:  consumerPool,
		exchanges:     exchanges,
	}

	// Инициализируем обменники
	if err := amqpConn.initExchanges(); err != nil {
		return nil, err
	}

	return amqpConn, nil
}

// Метод для инициализации всех обменников
func (c *amqpConnection) initExchanges() error {
	channel, err := c.publisherPool.GetChannel()
	if err != nil {
		return err
	}
	defer c.publisherPool.ReturnChannel(channel)

	for _, exchange := range c.exchanges {
		if err := c.declareExchange(channel, exchange.Name, exchange.Type); err != nil {
			return err
		}
	}
	return nil
}

// Метод публикации
func (c *amqpConnection) Publish(ctx context.Context, exchangeName, routingKey string, message []byte) error {
	channel, err := c.publisherPool.GetChannel()
	if err != nil {
		return err
	}
	defer c.publisherPool.ReturnChannel(channel)

	return channel.PublishWithContext(ctx,
		exchangeName,
		routingKey,
		false,
		false,
		amqp091.Publishing{
			ContentType: "application/json",
			Body:        message,
		})
}

// Метод для создания потребителя
func (c *amqpConnection) Consume(ctx context.Context, exchangeName, topic string) (<-chan UnitOfWork, error) {
	channel, err := c.consumerPool.GetChannel()
	if err != nil {
		return nil, err
	}

	queue, err := c.declareQueue(channel, topic)
	if err != nil {
		c.consumerPool.ReturnChannel(channel)
		return nil, err
	}

	err = c.bindQueueToExchange(channel, queue.Name, exchangeName, topic)
	if err != nil {
		c.consumerPool.ReturnChannel(channel)
		return nil, err
	}

	messages, err := channel.Consume(
		queue.Name,
		"",
		false, // autoAck=false для ручного подтверждения
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		c.consumerPool.ReturnChannel(channel)
		return nil, err
	}

	out := make(chan UnitOfWork)
	go func() {
		defer close(out)
		defer c.consumerPool.ReturnChannel(channel)

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-messages:
				if !ok {
					return
				}
				out <- NewAMQPUnitOfWork(&msg)
			}
		}
	}()

	return out, nil
}

// Метод для закрытия соединения
func (c *amqpConnection) Close() error {
	if err := c.publisherPool.Close(); err != nil {
		return err
	}
	if err := c.consumerPool.Close(); err != nil {
		return err
	}
	return c.connection.Close()
}

// Приватный метод для объявления обменника
func (c *amqpConnection) declareExchange(channel *amqp091.Channel, exchangeName, exchangeType string) error {
	return channel.ExchangeDeclare(
		exchangeName,
		exchangeType,
		true,  // durable
		false, // autoDelete
		false, // internal
		false, // noWait
		nil,   // args
	)
}

// Приватный метод для объявления очереди
func (c *amqpConnection) declareQueue(channel *amqp091.Channel, name string) (amqp091.Queue, error) {
	return channel.QueueDeclare(
		name,
		true,
		false,
		false,
		false,
		nil,
	)
}

// Приватный метод для привязки очереди к exchange
func (c *amqpConnection) bindQueueToExchange(channel *amqp091.Channel, queueName, exchangeName, topic string) error {
	return channel.QueueBind(
		queueName,
		topic,
		exchangeName,
		false,
		nil,
	)
}
