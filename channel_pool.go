package netherway

import (
	"errors"
	"sync"

	"github.com/rabbitmq/amqp091-go"
)

type ChannelPool struct {
	mu       sync.Mutex
	channels chan *amqp091.Channel
	conn     *amqp091.Connection
}

func NewChannelPool(conn *amqp091.Connection, size int) (*ChannelPool, error) {
	channels := make(chan *amqp091.Channel, size)
	for i := 0; i < size; i++ {
		channel, err := conn.Channel()
		if err != nil {
			return nil, err
		}
		channels <- channel
	}
	return &ChannelPool{channels: channels, conn: conn}, nil
}

func (p *ChannelPool) GetChannel() (*amqp091.Channel, error) {
	select {
	case ch := <-p.channels:
		return ch, nil
	default:
		return nil, errors.New("no available channels in the pool")
	}
}

func (p *ChannelPool) ReturnChannel(channel *amqp091.Channel) {
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case p.channels <- channel:
		// Channel returned to the pool
	default:
		// Pool is full, close the channel
		_ = channel.Close()
	}
}

func (p *ChannelPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	close(p.channels)
	for ch := range p.channels {
		_ = ch.Close()
	}
	return nil
}
