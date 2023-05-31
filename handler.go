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
	scheme, codecScheme, err := parseScheme(url)
	if err != nil {
		return nil, fmt.Errorf("parsing scheme for %q: %+v", url, err)
	}
	codec := defaultCodec
	if codecScheme != "" {
		if codec, err = DefaultURLMux.GetCodec(codecScheme); err != nil {
			return nil, fmt.Errorf("getting codec %s: %+v", codecScheme, err)
		}
	}
	handler, err := DefaultURLMux.GetHandler(scheme)
	if err != nil {
		return nil, fmt.Errorf("getting handler fo %s: %+v", scheme, err)
	}
	res, err := handler(ctx, url, func(ctx context.Context, msg Message) error {
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
	if err != nil {
		return nil, fmt.Errorf("instantiating handler for %q: %+v", url, err)
	}
	return res, nil
}
