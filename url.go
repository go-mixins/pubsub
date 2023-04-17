package pubsub

import (
	"fmt"
	"net/url"
	"strings"
)

func parseScheme(u *url.URL) (scheme, codec string, err error) {
	data := strings.SplitN(u.Scheme, "+", 3)
	switch len(data) {
	case 0:
		return "", "", fmt.Errorf("URL scheme must not be empty")
	case 1:
		return data[0], "", nil
	case 2:
		u.Scheme = data[0]
		return data[0], data[1], nil
	default:
		u.Scheme = data[2]
		return data[0], data[1], nil
	}
}
