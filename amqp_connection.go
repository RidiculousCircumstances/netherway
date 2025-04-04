package netherway

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type Exchange struct {
	Name string
	Type string
}

type amqpConnection struct {
	connection    *amqp091.Connection
	publisherPool *ChannelPool
	exchanges     []Exchange

	mu       sync.RWMutex
	isClosed bool
}

func NewAMQPConnection(
	conn *amqp091.Connection,
	publisherPoolSize int,
	exchanges []Exchange,
) (Connection, error) {

	publisherPool, err := NewChannelPool(conn, publisherPoolSize)
	if err != nil {
		return nil, err
	}

	amqpConn := &amqpConnection{
		connection:    conn,
		publisherPool: publisherPool,
		exchanges:     exchanges,
	}

	if err := amqpConn.initExchanges(); err != nil {
		return nil, err
	}

	go amqpConn.handleConnectionClose()

	return amqpConn, nil
}

// initExchanges объявляет все заданные exchange
func (c *amqpConnection) initExchanges() error {
	ch, err := c.publisherPool.GetChannel()
	if err != nil {
		return err
	}
	defer c.publisherPool.ReturnChannel(ch)

	for _, exchange := range c.exchanges {
		if err := c.declareExchange(ch, exchange.Name, exchange.Type); err != nil {
			// Если ошибка типа PRECONDITION_FAILED (не совпали параметры) - логируем отдельно
			log.Printf("[AMQP] Exchange declare error: %v", err)
			return err
		}
	}
	return nil
}

func (c *amqpConnection) handleConnectionClose() {
	ch := c.connection.NotifyClose(make(chan *amqp091.Error))
	err, ok := <-ch
	if !ok {
		// Канал закрыт без ошибки
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.isClosed = true
	log.Printf("[AMQP] connection closed, reason: %v\n", err)
}

func (c *amqpConnection) Publish(ctx context.Context, exchangeName, routingKey string, message []byte) error {
	c.mu.RLock()
	if c.isClosed {
		c.mu.RUnlock()
		return errors.New("AMQP connection is closed")
	}
	c.mu.RUnlock()

	ch, err := c.publisherPool.GetChannel()
	if err != nil {
		return err
	}
	defer c.publisherPool.ReturnChannel(ch)

	return ch.PublishWithContext(ctx,
		exchangeName,
		routingKey,
		false,
		false,
		amqp091.Publishing{
			ContentType: "application/json",
			Body:        message,
		})
}

// Consume объявляет очередь "вслепую".
// Добавляем проверку параметров, ловим PRECONDITION_FAILED для не совпадающих атрибутов
func (c *amqpConnection) Consume(ctx context.Context, exchangeName, topic string) (<-chan UnitOfWork, error) {
	c.mu.RLock()
	if c.isClosed {
		c.mu.RUnlock()
		return nil, errors.New("AMQP connection is closed")
	}
	c.mu.RUnlock()

	channel, err := c.connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// QoS
	if err := channel.Qos(10, 0, false); err != nil {
		_ = channel.Close()
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	// Чтобы проверить совпадение параметров, можно попробовать Passive Declare:
	// 1) Попробовать QueueDeclarePassive
	// 2) Если 404 (NOT_FOUND) => делаем обычный Declare
	// 3) Если 406 (PRECONDITION_FAILED) => логируем конфликт

	_, errPassive := channel.QueueDeclarePassive(
		topic,
		true,
		false,
		false,
		false,
		nil,
	)
	if errPassive != nil {
		var amqpErr *amqp091.Error
		isAmqpErr := errors.As(errPassive, &amqpErr)
		if isAmqpErr && amqpErr.Code == 404 {
			// Очередь не найдена => создаём
			if _, err2 := c.declareQueue(channel, topic); err2 != nil {
				_ = channel.Close()
				return nil, fmt.Errorf("create queue error: %w", err2)
			}
		} else if isAmqpErr && amqpErr.Code == 406 {
			log.Printf("[AMQP] Queue param mismatch (PRECONDITION_FAILED): %v", errPassive)
			_ = channel.Close()
			return nil, errPassive
		} else {
			// другая ошибка
			_ = channel.Close()
			return nil, errPassive
		}
	}

	// Привязка
	if err := c.bindQueueToExchange(channel, topic, exchangeName, topic); err != nil {
		_ = channel.Close()
		return nil, err
	}

	messages, err := channel.Consume(
		topic,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = channel.Close()
		return nil, err
	}

	out := make(chan UnitOfWork)

	go func() {
		defer close(out)
		defer func(ch *amqp091.Channel) {
			time.Sleep(500 * time.Millisecond)
			_ = ch.Close()
		}(channel)

		// Ловим закрытие канала через NotifyClose, чтобы логировать amqp.Error
		closeChan := make(chan *amqp091.Error, 1)
		channel.NotifyClose(closeChan)

		for {
			select {
			case <-ctx.Done():
				log.Printf("[Consumer] context cancelled, stop receiving. topic=%s exchange=%s", topic, exchangeName)
				return
			case amqpErr := <-closeChan:
				if amqpErr != nil {
					log.Printf("[Consumer] channel closed by error: %v", amqpErr)
				}
				return
			case msg, ok := <-messages:
				if !ok {
					log.Printf("[Consumer] messages chan closed (topic=%s)", topic)
					return
				}
				out <- NewAMQPUnitOfWork(&msg)
			}
		}
	}()
	return out, nil
}

func (c *amqpConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClosed {
		return nil
	}
	c.isClosed = true

	if err := c.publisherPool.Close(); err != nil {
		return err
	}
	return c.connection.Close()
}

func (c *amqpConnection) declareExchange(channel *amqp091.Channel, exchangeName, exchangeType string) error {
	return channel.ExchangeDeclare(
		exchangeName,
		exchangeType,
		true,
		false,
		false,
		false,
		nil,
	)
}

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

func (c *amqpConnection) bindQueueToExchange(channel *amqp091.Channel, queueName, exchangeName, routingKey string) error {
	return channel.QueueBind(
		queueName,
		routingKey,
		exchangeName,
		false,
		nil,
	)
}
