package netherway

import (
	"crypto/tls"
	"github.com/rabbitmq/amqp091-go"
	"net"
	"time"
)

// ConnConfig представляет настройки для соединения с AMQP
type ConnConfig struct {
	SASL                      []amqp091.Authentication
	Vhost                     string
	ChannelMax                uint16
	FrameSize                 int
	Heartbeat                 time.Duration
	TLSClientConfig           *tls.Config
	Properties                amqp091.Table
	Locale                    string
	Dial                      func(network, addr string) (net.Conn, error)
	AmqpURI                   string
	PublisherChannelPoolSize  int
	SubscriberChannelPoolSize int
	Exchanges                 []Exchange
}

// connFactory представляет фабрику для создания соединений с RabbitMQ
type connFactory struct {
	config ConnConfig
}

// NewConnFactory создает новый объект connFactory с переданным конфигом
func NewConnFactory(config ConnConfig) ConnFactory {
	return &connFactory{
		config: config,
	}
}

// GetConnection устанавливает соединение с RabbitMQ и возвращает объект amqpConnection
func (cf *connFactory) GetConnection() (Connection, error) {
	// Устанавливаем соединение с RabbitMQ
	conn, err := amqp091.DialConfig(cf.config.AmqpURI, amqp091.Config{
		SASL:            cf.config.SASL,
		Vhost:           cf.config.Vhost,
		ChannelMax:      cf.config.ChannelMax,
		FrameSize:       cf.config.FrameSize,
		Heartbeat:       cf.config.Heartbeat,
		TLSClientConfig: cf.config.TLSClientConfig,
		Properties:      cf.config.Properties,
		Locale:          cf.config.Locale,
		Dial:            cf.config.Dial,
	})
	if err != nil {
		return nil, err
	}

	// Создаем объект amqpConnection
	amqpConn, err := NewAMQPConnection(conn, cf.config.PublisherChannelPoolSize, cf.config.SubscriberChannelPoolSize, cf.config.Exchanges)

	if err != nil {
		return nil, err
	}

	return amqpConn, nil
}
