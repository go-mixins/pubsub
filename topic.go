package pubsub

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/go-mixins/metadata"
)

type Topic[A any] interface {
	Send(ctx context.Context, req A) error
	Shutdown(ctx context.Context) error
}

type topic[A any] struct {
	Topic[Message]
	codec Codec
}

func (t topic[A]) Send(ctx context.Context, req A) error {
	msg := Message{Metadata: make(map[string]string)}
	for k, v := range metadata.From(ctx) {
		msg.Metadata[k] = strings.Join(v, "|")
	}
	var err error
	if msg.Body, err = t.codec.Marshal(req); err != nil {
		return err
	}
	return t.Topic.Send(ctx, msg)
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
		if codec, err = DefaultURLMux.GetCodec(data[0]); err != nil {
			return nil, err
		}
		data[0] = data[1]
	}
	opener, err := DefaultURLMux.GetTopicOpener(data[0])
	if err != nil {
		return nil, err
	}
	t, err := opener(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("opening topic: %+v", err)
	}
	return topic[A]{Topic: t, codec: codec}, nil
}
