package netherway

import (
	"context"
	"go.uber.org/zap"
)

type Connection interface {
	Publish(ctx context.Context, exchangeName, routingKey string, message []byte) error
	Consume(ctx context.Context, exchangeName, topic string) (<-chan UnitOfWork, error)
	Close() error
}

type MessageBroker interface {
	Publish(ctx context.Context, exchangeName, topic string, data interface{}) error
	Subscribe(ctx context.Context, exchangeName, topic string, handler MessageHandler) error
	Pause(consumerKeys []string)
	Resume(ctx context.Context, consumerKeys []string) error
	Close() error
}

type Publisher interface {
	Publish(ctx context.Context, exchangeName, topic string, data interface{}) error
	Close() error
}

type Subscriber interface {
	Subscribe(ctx context.Context, exchangeName, topic string, handler MessageHandler) error
	Cancel()
}

type MessageHandler interface {
	Handle(ctx context.Context, msg []byte) (bool, error)
}

type ConnFactory interface {
	GetConnection() (Connection, error)
}

// Serializer интерфейс для сериализации и десериализации
type Serializer interface {
	Serialize(data interface{}) ([]byte, error)
	Deserialize(data []byte, v interface{}) error
}

type UnitOfWork interface {
	Nack(requeue bool) error
	Ack() error
	GetPayload() []byte
}

type Logger interface {
	Debug(msg string, fields ...zap.Field)
	Info(msg string, fields ...zap.Field)
	Warn(msg string, fields ...zap.Field)
	Error(msg string, fields ...zap.Field)
	Fatal(msg string, fields ...zap.Field)
	With(fields ...zap.Field) *zap.Logger
}
