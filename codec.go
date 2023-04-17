package pubsub

import (
	"encoding/json"
)

type JSONCodec struct{}

var defaultCodec Codec = JSONCodec{}

func (c JSONCodec) Marshal(src any) ([]byte, error) {
	return json.Marshal(src)
}

func (c JSONCodec) Unmarshal(data []byte, dest any) error {
	return json.Unmarshal(data, dest)
}

func init() {
	DefaultURLMux.RegisterCodec("json", defaultCodec)
}
