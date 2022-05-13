package pubsub

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/go-mixins/metadata"
	"gocloud.dev/pubsub"
)

// Global coder/decoder
var (
	Decoder func([]byte, any) error   = json.Unmarshal
	Encoder func(any) ([]byte, error) = json.Marshal
)

func decode[A any](ctx context.Context, msg *pubsub.Message) (context.Context, A, error) {
	var res A
	md := metadata.From(ctx)
	if md == nil {
		md = make(http.Header)
	}
	for k, v := range msg.Metadata {
		md[k] = strings.Split(v, "|")
	}
	if err := Decoder(msg.Body, &res); err != nil {
		return ctx, res, err
	}
	return metadata.With(ctx, md), res, nil
}

func encode[A any](ctx context.Context, req A) (*pubsub.Message, error) {
	msg := &pubsub.Message{Metadata: make(map[string]string)}
	md := metadata.From(ctx)
	for k, v := range md {
		msg.Metadata[k] = strings.Join(v, "|")
	}
	var err error
	if msg.Body, err = Encoder(req); err != nil {
		return nil, err
	}
	return msg, nil
}
