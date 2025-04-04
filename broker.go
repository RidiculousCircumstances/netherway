package netherway

import (
	"context"
	"sync"

	"go.uber.org/zap"
)

type PubFactory func(conn Connection) Publisher
type SubFactory func(conn Connection) Subscriber

type subscriptionConfig struct {
	exchange string
	topic    string
	handler  MessageHandler
}

type messageBroker struct {
	publisherFactory  PubFactory
	subscriberFactory SubFactory
	conn              Connection
	publisher         Publisher

	subscribers   map[string][]Subscriber       // Изменение: список подписчиков для каждого ключа
	subConfigs    map[string]subscriptionConfig // Настройки подписок
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
		subscribers:       make(map[string][]Subscriber), // Инициализация слайса подписчиков
		subConfigs:        make(map[string]subscriptionConfig),
	}
}

func (mb *messageBroker) lazyInitPublisher() error {
	mb.publisherMux.Lock()
	defer mb.publisherMux.Unlock()

	if mb.publisher != nil {
		return nil
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

// Subscribe с поддержкой нескольких подписчиков для одного топика
func (mb *messageBroker) Subscribe(ctx context.Context, exchangeName, topic string, handler MessageHandler) error {
	key := exchangeName + ":" + topic

	mb.subscriberMux.Lock()
	defer mb.subscriberMux.Unlock()

	// Сохраняем конфиг подписки
	mb.subConfigs[key] = subscriptionConfig{
		exchange: exchangeName,
		topic:    topic,
		handler:  handler,
	}

	// Создаём новый подписчик
	sub := mb.subscriberFactory(mb.conn)

	// Добавляем нового подписчика в список
	mb.subscribers[key] = append(mb.subscribers[key], sub)

	// Подписываем на топик
	if err := sub.Subscribe(ctx, exchangeName, topic, handler); err != nil {
		return err
	}

	mb.logger.Info("Subscription added", zap.String("subscriber", key))

	return nil
}

// Pause "отключает" подписки = полностью закрывает их и удаляет из brokers.subscribers
func (mb *messageBroker) Pause(consumerKeys []string) {
	mb.subscriberMux.Lock()
	defer mb.subscriberMux.Unlock()

	var keysToPause []string
	if len(consumerKeys) == 0 {
		for key := range mb.subscribers {
			keysToPause = append(keysToPause, key)
		}
	} else {
		keysToPause = consumerKeys
	}

	// Приостанавливаем работу подписчиков
	for _, key := range keysToPause {
		subs, exists := mb.subscribers[key]
		if !exists {
			mb.logger.Warn("Unknown subscriber to pause", zap.String("subscriber", key))
			continue
		}

		for _, sub := range subs {
			mb.logger.Info("Pausing subscription", zap.String("subscriber", key))
			sub.Cancel() // Закрываем подписчика
		}

		delete(mb.subscribers, key)
	}
}

// Resume восстанавливает подписчиков для указанных ключей
func (mb *messageBroker) Resume(ctx context.Context, consumerKeys []string) error {
	mb.subscriberMux.Lock()
	defer mb.subscriberMux.Unlock()

	var keysToResume []string
	if len(consumerKeys) == 0 {
		// Возобновляем все подписки
		for key := range mb.subConfigs {
			keysToResume = append(keysToResume, key)
		}
	} else {
		keysToResume = consumerKeys
	}

	// Возобновляем подписчиков
	for _, key := range keysToResume {
		cfg, existsCfg := mb.subConfigs[key]
		if !existsCfg {
			mb.logger.Warn("No subscription config found for resume", zap.String("subscriber", key))
			continue
		}

		// Пересоздаём подписчика
		sub := mb.subscriberFactory(mb.conn)
		if err := sub.Subscribe(ctx, cfg.exchange, cfg.topic, cfg.handler); err != nil {
			mb.logger.Error("Error resubscribing", zap.String("subscriber", key), zap.Error(err))
			return err
		}

		// Добавляем нового подписчика в список
		mb.subscribers[key] = append(mb.subscribers[key], sub)

		mb.logger.Info("Resuming subscription", zap.String("subscriber", key))
	}

	return nil
}

func (mb *messageBroker) Close() error {
	var pubErr error
	if mb.publisher != nil {
		pubErr = mb.publisher.Close()
	}

	mb.subscriberMux.Lock()
	defer mb.subscriberMux.Unlock()

	// Закрываем всех подписчиков
	for _, subs := range mb.subscribers {
		for _, sub := range subs {
			sub.Cancel()
		}
	}

	if pubErr != nil {
		return pubErr
	}
	return nil
}
