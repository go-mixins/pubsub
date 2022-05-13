package pubsub

import (
	"context"
	"fmt"

	"github.com/go-mixins/log"
	"gocloud.dev/pubsub"
)

type Handler[A any] func(ctx context.Context, msg A) error

func (h Handler[A]) Connect(ctx context.Context, url string) error {
	return h.connect(ctx, url, 1)
}

func (h Handler[A]) ConnectConcurrent(ctx context.Context, url string, concurrency int) error {
	return h.connect(ctx, url, concurrency)
}

func (h Handler[A]) connect(ctx context.Context, url string, concurrency int) error {
	sub, err := pubsub.OpenSubscription(ctx, url)
	if err != nil {
		return fmt.Errorf("opening subscription: %+v", err)
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
				ctx, req, err := decode[A](ctx, msg)
				if err != nil {
					return fmt.Errorf("decoding message: %+v", err)
				}
				return h(ctx, req)
			}()
		}
		for n := 0; n < concurrency; n++ {
			sem <- struct{}{}
		}
	}()
	return nil
}
