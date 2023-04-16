package pubsub

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/go-mixins/metadata"
)

func Handle[A any](ctx context.Context, url string, h HandlerFunc[A]) (Subscription, error) {
	return HandleConcurrent[A](ctx, url, h, 1)
}

func HandleConcurrent[A any](ctx context.Context, u string, h HandlerFunc[A], concurrency int) (Subscription, error) {
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
	handler, err := DefaultURLMux.GetHandler(data[0])
	if err != nil {
		return nil, err
	}
	return handler(ctx, url, func(ctx context.Context, msg Message) error {
		var req A
		md := metadata.From(ctx)
		if md == nil {
			md = make(http.Header)
		}
		for k, v := range msg.Metadata {
			md[k] = strings.Split(v, "|")
		}
		if err := codec.Unmarshal(msg.Body, &req); err != nil {
			return err
		}
		return h(metadata.With(ctx, md), req)
	}, concurrency)
}
