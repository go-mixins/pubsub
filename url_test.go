package pubsub

import (
	"bytes"
	"fmt"
	"net/url"
	"testing"

	"github.com/andviro/goldie"
)

func TestURL_Parse(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range []string{
		"nsq+json://10.0.0.1:4151,10.0.0.2:4151/topic",
		"gocloud+json+nats://10.0.0.1:4151,10.0.0.2:4151/topic#channel",
	} {
		u, err := url.Parse(tc)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		scheme, codec, err := parseScheme(u)
		t.Logf("%s: %s %s %s %+v\n", tc, scheme, codec, u.Scheme, err)
		fmt.Fprintf(buf, "%s: %s %s %s %+v\n", tc, scheme, codec, u.Scheme, err)
	}
	goldie.Assert(t, "url-parse", buf.Bytes())
}
