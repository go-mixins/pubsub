package gocloud

import (
	"context"
	"fmt"
	"net/url"

	"github.com/go-mixins/log"
	"github.com/go-mixins/pubsub"
	p "gocloud.dev/pubsub"
)

func HandleConcurrent(ctx context.Context, url *url.URL, h pubsub.HandlerFunc[pubsub.Message], concurrency int) (pubsub.Subscription, error) {
	sub, err := p.DefaultURLMux().OpenSubscriptionURL(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("opening subscription: %+v", err)
	}
	sem := make(chan struct{}, concurrency)
	go func() {
	recvLoop:
		for {
			msg, err := sub.Receive(ctx)
			if err != nil {
				log.Get(ctx).Errorf("receiving message: %+v", err)
				break
			}
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				break recvLoop
			}
			go func() (rErr error) {
				defer func() { <-sem }()
				defer msg.Ack()
				return h(ctx, pubsub.Message{Body: msg.Body, Metadata: msg.Metadata})
			}()
		}
		for n := 0; n < concurrency; n++ {
			sem <- struct{}{}
		}
	}()
	return sub, nil
}

func init() {
	pubsub.DefaultURLMux.RegisterHandler("gocloud", HandleConcurrent)
}
