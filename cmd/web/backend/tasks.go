package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"grain/nexus"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

type TaskStateStreamer struct {
	subscribers map[chan []byte]struct{}
	mu          sync.RWMutex
}

func NewTaskStateStreamer() *TaskStateStreamer {
	return &TaskStateStreamer{
		subscribers: make(map[chan []byte]struct{}),
	}
}

func (s *TaskStateStreamer) Start(ctx context.Context, logger zerolog.Logger, queue nexus.TaskStateQueue) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			state, err := queue.PullNewTaskState(pollCtx)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				logger.Error().Err(err).Msg("failed to pull task state")
				time.Sleep(200 * time.Millisecond)
				continue
			}
			if state == nil {
				time.Sleep(200 * time.Millisecond)
				continue
			}

			data, err := json.Marshal(state)
			if err != nil {
				logger.Error().Err(err).Msg("failed to marshal task state")
				continue
			}

			s.broadcast(data)
		}
	}
}

func (s *TaskStateStreamer) StreamFiber(c *fiber.Ctx) error {
	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")
	c.Set("Connection", "keep-alive")
	c.Set("X-Accel-Buffering", "no")

	msgCh := s.subscribe()

	c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
		defer s.unsubscribe(msgCh)

		keepAlive := time.NewTicker(20 * time.Second)
		defer keepAlive.Stop()

		defer keepAlive.Stop()

		for {
			select {
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				if _, err := w.WriteString("event: task_state\n"); err != nil {
					return
				}
				if _, err := fmt.Fprintf(w, "data: %s\n\n", msg); err != nil {
					return
				}
				if err := w.Flush(); err != nil {
					return
				}
			case <-keepAlive.C:
				if _, err := w.WriteString(": keep-alive\n\n"); err != nil {
					return
				}
				if err := w.Flush(); err != nil {
					return
				}
			}
		}
	})

	return nil
}

func (s *TaskStateStreamer) subscribe() chan []byte {
	ch := make(chan []byte, 32)
	s.mu.Lock()
	s.subscribers[ch] = struct{}{}
	s.mu.Unlock()
	return ch
}

func (s *TaskStateStreamer) unsubscribe(ch chan []byte) {
	s.mu.Lock()
	delete(s.subscribers, ch)
	close(ch)
	s.mu.Unlock()
}

func (s *TaskStateStreamer) broadcast(msg []byte) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for ch := range s.subscribers {
		select {
		case ch <- msg:
		default:
		}
	}
}
