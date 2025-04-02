package netherway

import (
	"context"
	"go.uber.org/zap"
	"sync"
)

type subscriber struct {
	connection   Connection
	logger       Logger
	exchangeName string
	topic        string
	handler      MessageHandler

	cancelFunc context.CancelFunc
	paused     bool
	mu         sync.Mutex
}

func NewSubscriber(conn Connection, logger Logger) Subscriber {
	return &subscriber{
		connection: conn,
		logger:     logger,
	}
}

func (s *subscriber) Subscribe(ctx context.Context, exchangeName, topic string, handler MessageHandler) error {
	s.mu.Lock()
	s.exchangeName = exchangeName
	s.topic = topic
	s.handler = handler
	s.mu.Unlock()
	return s.startConsuming(ctx)
}

// startConsuming создаёт подписку и запускает обработку сообщений.
func (s *subscriber) startConsuming(parentCtx context.Context) error {
	ctx, cancel := context.WithCancel(parentCtx)
	s.mu.Lock()
	s.cancelFunc = cancel
	s.paused = false
	s.mu.Unlock()

	messages, err := s.connection.Consume(ctx, s.exchangeName, s.topic)
	if err != nil {
		return err
	}
	go s.processMessages(ctx, messages)
	return nil
}

func (s *subscriber) processMessages(ctx context.Context, messages <-chan UnitOfWork) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}
			// Запускаем обработку сообщения в отдельной горутине
			go func(m UnitOfWork) {
				ack, err := s.handler.Handle(ctx, m.GetPayload())
				if err != nil {
					s.logger.Error("Error processing message", zap.Error(err))
					_ = m.Nack(false)
					return
				}
				if ack {
					if err := m.Ack(); err != nil {
						s.logger.Error("Error acknowledging message", zap.Error(err))
					}
				} else {
					s.logger.Warn("Message not acknowledged")
					_ = m.Nack(true)
				}
			}(msg)
		}
	}
}

// Cancel прекращает текущую подписку
func (s *subscriber) Cancel() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancelFunc != nil {
		s.cancelFunc()
		s.cancelFunc = nil
		s.paused = true
		s.logger.Info("Subscriber paused")
	}
}

// Resume восстанавливает подписку с теми же параметрами
func (s *subscriber) Resume(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.paused {
		s.logger.Info("Subscriber not paused, no need to resume")
		return nil
	}
	s.logger.Info("Resuming subscriber")
	return s.startConsuming(ctx)
}

func (s *subscriber) Close() error {
	s.Cancel()
	return s.connection.Close()
}
