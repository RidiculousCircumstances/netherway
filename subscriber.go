package netherway

import (
	"context"
	"errors"
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

	mu      sync.Mutex
	paused  bool
	started bool // флаг: был ли уже вызван startConsuming?

	wg       sync.WaitGroup // для "полугрейсфул" остановки (считаем in-flight)
	doneChan chan struct{}  // закрывается, когда processMessages полностью завершается
}

func NewSubscriber(conn Connection, logger Logger) Subscriber {
	return &subscriber{
		connection: conn,
		logger:     logger,
	}
}

func (s *subscriber) Subscribe(ctx context.Context, exchangeName, topic string, handler MessageHandler) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		s.logger.Error("startConsuming called more than once on the same subscriber",
			zap.String("exchange", exchangeName), zap.String("topic", topic))
		return errors.New("subscriber already started")
	}

	s.exchangeName = exchangeName
	s.topic = topic
	s.handler = handler
	s.started = true

	return s.startConsuming(ctx)
}

// startConsuming создаёт подписку и запускает обработку сообщений.
func (s *subscriber) startConsuming(parentCtx context.Context) error {
	ctx, cancel := context.WithCancel(parentCtx)
	s.cancelFunc = cancel
	s.paused = false

	messages, err := s.connection.Consume(ctx, s.exchangeName, s.topic)
	if err != nil {
		return err
	}

	s.doneChan = make(chan struct{}) // инициализируем doneChan для нового консьюмера

	go s.processMessages(ctx, messages)
	return nil
}

func (s *subscriber) processMessages(ctx context.Context, messages <-chan UnitOfWork) {
	defer close(s.doneChan) // горутина выходит => сигналим "окончание" Cancel'у

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Context done, waiting for in-flight messages",
				zap.String("exchange", s.exchangeName),
				zap.String("topic", s.topic))
			// Ждём, пока все горутины обработки завершат работу
			s.wg.Wait()
			s.logger.Info("All messages done, exiting processMessages")
			return

		case msg, ok := <-messages:
			if !ok {
				// Канал закрыт (AMQP отключился?) — тоже выходим
				s.logger.Info("Messages channel closed",
					zap.String("exchange", s.exchangeName),
					zap.String("topic", s.topic))
				s.wg.Wait()
				return
			}

			// Счётчик in-flight
			s.wg.Add(1)
			err := s.handleMessage(ctx, msg)
			s.wg.Done()

			if err != nil {
				s.logger.Error("Error in handleMessage", zap.Error(err))
			}
		}
	}
}

func (s *subscriber) handleMessage(ctx context.Context, m UnitOfWork) error {
	ack, err := s.handler.Handle(ctx, m.GetPayload())
	if err != nil {
		s.logger.Error("Error processing message", zap.Error(err))
		_ = m.Nack(false)
		return err
	}
	if ack {
		if e := m.Ack(); e != nil {
			s.logger.Error("Error acknowledging message", zap.Error(e))
		}
	} else {
		s.logger.Warn("Message not acknowledged")
		_ = m.Nack(true)
	}
	return nil
}

// Cancel прекращает текущую подписку (блокирующе, пока processMessages не выйдет)
func (s *subscriber) Cancel() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cancelFunc != nil {
		s.logger.Info("Canceling subscriber (blocking)",
			zap.String("exchange", s.exchangeName),
			zap.String("topic", s.topic))

		// Останавливаем контекст
		s.cancelFunc()
		s.cancelFunc = nil
		s.paused = true
	} else {
		// уже отменён или не запущен
		return
	}

	// Если startConsuming не был вызыван, нет doneChan
	if !s.started || s.doneChan == nil {
		return
	}

	// Здесь освобождаем мьютекс, чтобы не держать лок при ожидании doneChan
	doneChan := s.doneChan
	s.mu.Unlock()

	// Ждём, пока горутина processMessages завершит цикл
	<-doneChan

	// Возвращаем лок перед выходом (так как мы делаем deferred unlock выше,
	// нужно заново взять лок)
	s.mu.Lock()
	s.logger.Info("Subscriber fully canceled",
		zap.String("exchange", s.exchangeName),
		zap.String("topic", s.topic))
}
