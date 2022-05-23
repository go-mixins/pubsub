package gocloud

import (
	"context"
	"fmt"
	"net/url"

	"github.com/go-mixins/pubsub"
	p "gocloud.dev/pubsub"
)

func OpenTopic(ctx context.Context, url *url.URL) (pubsub.Topic[*p.Message], error) {
	topic, err := p.DefaultURLMux().OpenTopicURL(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("opening topic: %+v", err)
	}
	return topic, nil
}

func init() {
	pubsub.DefaultURLMux.RegisterTopicOpener("gocloud", OpenTopic)
}
