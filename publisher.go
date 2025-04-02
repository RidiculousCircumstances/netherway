package netherway

import (
	"context"
	"encoding/json"
	"go.uber.org/zap"
)

type publisher struct {
	connection Connection // Используем интерфейс соединения
	logger     Logger     // Логгер
}

// NewPublisher создает нового паблишера, используя интерфейс соединения.
func NewPublisher(conn Connection, logger Logger) Publisher {
	return &publisher{
		connection: conn,
		logger:     logger,
	}
}

func (p *publisher) Publish(ctx context.Context, exchangeName, topic string, data interface{}) error {
	// Сериализация данных
	payload, err := json.Marshal(data)
	if err != nil {
		p.logger.Error("Failed to serialize data", zap.Error(err))
		return err
	}

	// Публикация сообщения через интерфейс соединения
	err = p.connection.Publish(ctx, exchangeName, topic, payload)
	if err != nil {
		p.logger.Error("Failed to publish message", zap.Error(err))
	}
	return err
}

func (p *publisher) Close() error {
	// Закрываем соединение через интерфейс
	return p.connection.Close()
}
