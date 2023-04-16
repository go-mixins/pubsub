package nsq

import (
	"context"
	"fmt"
	"net/url"

	"github.com/go-mixins/pubsub"
	"github.com/nsqio/go-nsq"
)

type subscription struct {
	consumer *nsq.Consumer
}

func (s *subscription) Shutdown(ctx context.Context) error {
	s.consumer.Stop()
	return nil
}

func HandleConcurrent(ctx context.Context, u *url.URL, handler pubsub.HandlerFunc[pubsub.Message], concurrency int) (pubsub.Subscription, error) {
	cfg, err := parseURL(u)
	if err != nil {
		return nil, err
	}
	if cfg.Channel == "" {
		cfg.Channel = "main"
	}
	consumer, err := nsq.NewConsumer(cfg.Topic, cfg.Channel, cfg.Config)
	if err != nil {
		return nil, fmt.Errorf("creating NSQ consumer: %+v", err)
	}
	consumer.AddConcurrentHandlers(nsq.HandlerFunc(func(msg *nsq.Message) error {
		env, err := unpackEnvelope(msg.Body)
		if err != nil {
			return nil
		}
		return handler(ctx, pubsub.Message{Body: env.Body, Metadata: env.Header})
	}), concurrency)
	if err := consumer.ConnectToNSQLookupds(cfg.Remotes); err != nil {
		return nil, fmt.Errorf("connecting to NSQLookupDs: %+v", err)
	}
	res := &subscription{
		consumer: consumer,
	}
	return res, nil
}

func init() {
	pubsub.DefaultURLMux.RegisterHandler("nsq", HandleConcurrent)
}
