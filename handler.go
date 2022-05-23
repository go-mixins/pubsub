package pubsub

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/go-mixins/metadata"
	"gocloud.dev/pubsub"
)

func Handle[A any](ctx context.Context, url string, h HandlerFunc[A]) error {
	return HandleConcurrent[A](ctx, url, h, 1)
}

func HandleConcurrent[A any](ctx context.Context, u string, h HandlerFunc[A], concurrency int) error {
	url, err := url.Parse(u)
	if err != nil {
		return err
	}
	data := strings.SplitN(url.Scheme, "+", 2)
	codec := defaultCodec
	if len(data) == 0 {
		return fmt.Errorf("URL scheme is empty string")
	} else if len(data) == 2 {
		if codec, err = DefaultURLMux.GetCodec(data[1]); err != nil {
			return err
		}
	}
	handler, err := DefaultURLMux.GetHandler(data[0])
	if err != nil {
		return err
	}
	return handler(ctx, url, func(ctx context.Context, msg *pubsub.Message) error {
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
