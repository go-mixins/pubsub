package pubsub

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/go-mixins/metadata"
	"gocloud.dev/pubsub"
)

type Topic[A any] interface {
	Send(ctx context.Context, req A) error
}

type sendFunc[A any] func(ctx context.Context, req A) error

func (t sendFunc[A]) Send(ctx context.Context, req A) error {
	return t(ctx, req)
}

func OpenTopic[A any](ctx context.Context, u string) (Topic[A], error) {
	url, err := url.Parse(u)
	if err != nil {
		return nil, err
	}
	data := strings.SplitN(url.Scheme, "+", 2)
	codec := defaultCodec
	if len(data) == 0 {
		return nil, fmt.Errorf("URL scheme is empty string")
	} else if len(data) == 2 {
		if codec, err = DefaultURLMux.GetCodec(data[1]); err != nil {
			return nil, err
		}
	}
	opener, err := DefaultURLMux.GetTopicOpener(data[0])
	if err != nil {
		return nil, err
	}
	t, err := opener(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("opening topic: %+v", err)
	}
	return sendFunc[A](func(ctx context.Context, req A) error {
		msg := &pubsub.Message{Metadata: make(map[string]string)}
		for k, v := range metadata.From(ctx) {
			msg.Metadata[k] = strings.Join(v, "|")
		}
		var err error
		if msg.Body, err = codec.Marshal(req); err != nil {
			return err
		}
		return t.Send(ctx, msg)
	}), nil
}
