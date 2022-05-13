package pubsub

import (
	"context"
	"fmt"

	"gocloud.dev/pubsub"
)

func Open[A any](ctx context.Context, url string) (*Topic[A], error) {
	topic, err := pubsub.OpenTopic(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("opening topic: %+v", err)
	}
	return (*Topic[A])(topic), nil
}

type Topic[A any] pubsub.Topic

func (t *Topic[A]) Send(ctx context.Context, req A) error {
	msg, err := encode(ctx, req)
	if err != nil {
		return fmt.Errorf("encoding message: %+v", err)
	}
	return (*pubsub.Topic)(t).Send(ctx, msg)
}
