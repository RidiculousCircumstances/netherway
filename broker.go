package netherway

import (
	"context"
	"go.uber.org/zap"
	"sync"
)

type PubFactory func(conn Connection) Publisher
type SubFactory func(conn Connection) Subscriber

type messageBroker struct {
	publisherFactory  PubFactory
	subscriberFactory SubFactory
	conn              Connection
	publisher         Publisher

	// Храним подписчиков в мапе (ключ: exchange:topic)
	subscribers   map[string]Subscriber
	subscriberMux sync.Mutex
	publisherMux  sync.Mutex

	logger Logger
}

type Config struct {
	PublisherFactory  PubFactory
	SubscriberFactory SubFactory
	ConnFactory       ConnFactory
	Logger            Logger
}

func NewMessageBroker(cfg Config) MessageBroker {
	conn, err := cfg.ConnFactory.GetConnection()
	if err != nil {
		panic(err)
	}
	return &messageBroker{
		publisherFactory:  cfg.PublisherFactory,
		subscriberFactory: cfg.SubscriberFactory,
		logger:            cfg.Logger,
		conn:              conn,
		subscribers:       make(map[string]Subscriber),
	}
}

// lazyInitPublisher инициализирует publisher при первом вызове
func (mb *messageBroker) lazyInitPublisher() error {
	mb.publisherMux.Lock()
	defer mb.publisherMux.Unlock()

	if mb.publisher != nil {
		return nil // Уже инициализирован
	}

	mb.publisher = mb.publisherFactory(mb.conn)
	return nil
}

// Publish отправляет сообщение в указанный топик
func (mb *messageBroker) Publish(ctx context.Context, exchangeName, topic string, data interface{}) error {
	if err := mb.lazyInitPublisher(); err != nil {
		return err
	}

	return mb.publisher.Publish(ctx, exchangeName, topic, data)
}

// Subscribe создаёт подписку и сохраняет её в мапе
func (mb *messageBroker) Subscribe(ctx context.Context, exchangeName, topic string, handler MessageHandler) error {
	key := exchangeName + ":" + topic
	mb.subscriberMux.Lock()
	defer mb.subscriberMux.Unlock()

	if _, exists := mb.subscribers[key]; exists {
		mb.logger.Warn("Subscription already exists", zap.String("subscriber", key))
		return nil
	}
	sub := mb.subscriberFactory(mb.conn)
	if err := sub.Subscribe(ctx, exchangeName, topic, handler); err != nil {
		return err
	}
	mb.subscribers[key] = sub
	mb.logger.Info("Subscription added", zap.String("subscriber: ", key))
	return nil
}

// Pause приостанавливает подписки.
// Если не передан ни один ключ, приостанавливаются все подписки.
// Если передан ключ (consumerKey[0]), приостанавливается только соответствующий подписчик.
func (mb *messageBroker) Pause(consumerKey ...string) {
	mb.subscriberMux.Lock()
	defer mb.subscriberMux.Unlock()

	if len(consumerKey) == 0 {
		// Приостанавливаем все подписки
		for key, sub := range mb.subscribers {
			mb.logger.Info("Pausing subscription", zap.String("subscriber", key))
			sub.Cancel()
		}
	} else {
		key := consumerKey[0]
		if sub, exists := mb.subscribers[key]; exists {
			mb.logger.Info("Pausing subscription", zap.String("subscriber", key))
			sub.Cancel()
		} else {
			mb.logger.Warn("Subscription not found", zap.String("subscriber", key))
		}
	}
}

// Resume возобновляет подписки.
// Если не передан ни один ключ, возобновляются все подписки.
// Если передан ключ (consumerKey[0]), возобновляется только подписка с указанным ключом.
func (mb *messageBroker) Resume(ctx context.Context, consumerKey ...string) error {
	mb.subscriberMux.Lock()
	defer mb.subscriberMux.Unlock()

	if len(consumerKey) == 0 {
		// Возобновляем все подписки
		for key, sub := range mb.subscribers {
			mb.logger.Info("Resuming subscription", zap.String("subscriber", key))
			if err := sub.Resume(ctx); err != nil {
				mb.logger.Error("Error resuming subscription", zap.String("subscriber", key), zap.Error(err))
				return err
			}
		}
	} else {
		key := consumerKey[0]
		if sub, exists := mb.subscribers[key]; exists {
			mb.logger.Info("Resuming subscription", zap.String("subscriber", key))
			if err := sub.Resume(ctx); err != nil {
				mb.logger.Error("Error resuming subscription", zap.String("subscriber", key), zap.Error(err))
				return err
			}
		} else {
			mb.logger.Warn("Subscription not found", zap.String("subscriber", key))
		}
	}
	return nil
}

func (mb *messageBroker) Close() error {
	var pubErr error

	if mb.publisher != nil {
		pubErr = mb.publisher.Close()
	}

	// Закрываем всех подписчиков из мапы subscribers
	mb.subscriberMux.Lock()
	defer mb.subscriberMux.Unlock()
	for key, sub := range mb.subscribers {
		if err := sub.Close(); err != nil {
			mb.logger.Error("Error closing subscriber", zap.String("subscriber", key), zap.Error(err))
		}
	}

	if pubErr != nil {
		return pubErr
	}
	return nil
}
