package gocloud

import (
	"context"
	"fmt"
	"net/url"

	"github.com/go-mixins/pubsub"
	p "gocloud.dev/pubsub"
)

type topic struct {
	*p.Topic
}

func (t topic) Send(ctx context.Context, req pubsub.Message) error {
	return t.Topic.Send(ctx, &p.Message{Body: req.Body, Metadata: req.Metadata})
}

func OpenTopic(ctx context.Context, url *url.URL) (pubsub.Topic[pubsub.Message], error) {

	t, err := p.DefaultURLMux().OpenTopicURL(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("opening topic: %+v", err)
	}
	return topic{t}, nil
}

func init() {
	pubsub.DefaultURLMux.RegisterTopicOpener("gocloud", OpenTopic)
}
