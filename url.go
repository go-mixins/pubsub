package pubsub

import (
	"fmt"
	"net/url"
	"strings"
)

func parseScheme(u *url.URL) (scheme, codec string, err error) {
	data := strings.SplitN(u.Scheme, "+", 3)
	if len(data) < 2 {
		return "", "", fmt.Errorf("URL scheme must contain at least one '+'")
	}
	if len(data) == 2 {
		u.Scheme = data[0]
		return data[0], data[1], nil
	}
	u.Scheme = data[2]
	return data[0], data[1], nil
}
