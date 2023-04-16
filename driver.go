package pubsub

import (
	"context"
	"fmt"
	"net/url"
)

type HandlerFunc[A any] func(ctx context.Context, msg A) error

type Handler[A any] func(ctx context.Context, url *url.URL, h HandlerFunc[A], concurrency int) (Subscription, error)

type Subscription interface {
	Shutdown(ctx context.Context) error
}

type Codec interface {
	Marshal(src any) ([]byte, error)
	Unmarshal(data []byte, dest any) error
}

type Message struct {
	Metadata map[string]string
	Body     []byte
}

type TopicOpener[A any] func(ctx context.Context, url *url.URL) (Topic[A], error)

type URLMux struct {
	handlerSchemes map[string]Handler[Message]
	topicSchemes   map[string]TopicOpener[Message]
	codecSchemes   map[string]Codec
}

func (m *URLMux) RegisterHandler(scheme string, h Handler[Message]) {
	if m.handlerSchemes == nil {
		m.handlerSchemes = make(map[string]Handler[Message])
	}
	m.handlerSchemes[scheme] = h
}

func (m *URLMux) RegisterCodec(scheme string, c Codec) {
	if m.codecSchemes == nil {
		m.codecSchemes = make(map[string]Codec)
	}
	m.codecSchemes[scheme] = c
}

func (m *URLMux) RegisterTopicOpener(scheme string, driver TopicOpener[Message]) {
	if m.topicSchemes == nil {
		m.topicSchemes = make(map[string]TopicOpener[Message])
	}
	m.topicSchemes[scheme] = driver
}

var DefaultURLMux = &URLMux{}

func (m *URLMux) GetHandler(scheme string) (Handler[Message], error) {
	res, ok := m.handlerSchemes[scheme]
	if !ok {
		return nil, fmt.Errorf("pubsub handler scheme %s not registered", scheme)
	}
	return res, nil
}

func (m *URLMux) GetTopicOpener(scheme string) (TopicOpener[Message], error) {
	res, ok := m.topicSchemes[scheme]
	if !ok {
		return nil, fmt.Errorf("pubsub topic scheme %s not registered", scheme)
	}
	return res, nil
}

func (m *URLMux) GetCodec(scheme string) (Codec, error) {
	res, ok := m.codecSchemes[scheme]
	if !ok {
		return nil, fmt.Errorf("pubsub codec scheme %s not registered", scheme)
	}
	return res, nil
}
