package publogsub

import (
	"encoding/json"
	"reflect"

	"golang.org/x/net/context"

	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/sirupsen/logrus"
)

func TestPublogsubHook_Fire(t *testing.T) {
	expected := Event{
		Message: "Hello world!",
		Data:    logrus.Fields{"hello": "world"},
		Level:   "INFO",
	}

	ctx := context.Background()
	client, _ := pubsub.NewClient(ctx, os.Getenv("GOOGLE_PROJECT_ID"))

	hook, _ := New(ctx, client, "publogsub-test")
	logrus.AddHook(hook)

	logrus.WithFields(expected.Data).Info(expected.Message)

	sub, _ := client.CreateSubscription(ctx, "publogsub-test", pubsub.SubscriptionConfig{
		Topic:       client.Topic("publogsub-test"),
		AckDeadline: 20 * time.Second,
	})

	defer func() { sub.Delete(ctx) }()

	var mu sync.Mutex

	received := false
	cctx, cancel := context.WithCancel(ctx)

	var event Event

	sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		mu.Lock()
		defer mu.Unlock()
		json.Unmarshal(msg.Data, &event)
		cancel()
		msg.Nack()

		received = true
	})

	if !received {
		t.Error("No event found in pubsub topic")
		return
	}

	if expected.Message != event.Message {
		t.Error("Message is different", expected.Message, event.Message)
	}

	if !reflect.DeepEqual(expected.Data, event.Data) {
		t.Error("Data is different", expected.Data, event.Data)
	}

	if expected.Level != event.Level {
		t.Error("Level is different", expected.Level, event.Level)
	}
}
