package publogsub

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/sirupsen/logrus"
)

// Hook is a logrus hook for pushing logs to Google Pub/Sub.
type Hook struct {
	ctx    context.Context
	client *pubsub.Client
	topic  *pubsub.Topic
}

// Event is the data structure published to Google Pub/Sub.
type Event struct {
	Timestamp string
	Message   string
	Data      logrus.Fields
	Level     string
}

// New instanciates a new pubsub Hook for logrus.
func New(ctx context.Context, client *pubsub.Client, name string) (*Hook, error) {
	topic := client.Topic(name)

	if ok, err := topic.Exists(ctx); err != nil {
		return nil, err
	} else if !ok {
		if topic, err = client.CreateTopic(ctx, name); err != nil {
			return nil, err
		}
	}

	topic.PublishSettings = pubsub.PublishSettings{
		DelayThreshold:    5 * time.Second,
		CountThreshold:    100,
		ByteThreshold:     1e6,
		BufferedByteLimit: 1e9,
	}

	return &Hook{
		ctx,
		client,
		topic,
	}, nil
}

// Fire logs an event.
func (h *Hook) Fire(entry *logrus.Entry) error {
	event := Event{
		Timestamp: entry.Time.UTC().Format(time.RFC3339Nano),
		Message:   entry.Message,
		Data:      entry.Data,
		Level:     strings.ToUpper(entry.Level.String()),
	}

	bytes, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := pubsub.Message{Data: bytes}
	h.topic.Publish(h.ctx, &msg)
	return nil
}

// Levels list log levels handled by the hook.
func (h *Hook) Levels() []logrus.Level {
	return logrus.AllLevels
}
