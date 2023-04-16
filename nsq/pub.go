package nsq

import (
	"context"
	"fmt"
	"net/url"

	"github.com/go-mixins/pubsub"
	"github.com/nsqio/go-nsq"
)

func OpenTopic(ctx context.Context, u *url.URL) (pubsub.Topic[pubsub.Message], error) {
	cfg, err := parseURL(u)
	if err != nil {
		return nil, err
	}
	if len(cfg.Remotes) == 0 {
		return nil, fmt.Errorf("NSQD address not specified in URL")
	}
	producer, err := nsq.NewProducer(cfg.Remotes[0], cfg.Config)
	if err != nil {
		return nil, err
	}
	res := topic{
		producer: producer,
		topic:    cfg.Topic,
	}
	return res, nil
}

type topic struct {
	producer *nsq.Producer
	topic    string
}

func (t topic) Shutdown(ctx context.Context) error {
	t.producer.Stop()
	return nil
}

func (t topic) Send(ctx context.Context, msg pubsub.Message) error {
	data, err := packEnvelope(envelope{
		Header: msg.Metadata,
		Body:   msg.Body,
	})
	if err != nil {
		return err
	}
	if err := t.producer.Publish(t.topic, data); err != nil {
		return err
	}
	return nil
}

func init() {
	pubsub.DefaultURLMux.RegisterTopicOpener("gocloud", OpenTopic)
}
