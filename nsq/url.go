package nsq

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/nsqio/go-nsq"
)

type config struct {
	Remotes []string
	Topic   string
	Channel string
	Config  *nsq.Config `json:"-"`
}

func parseURL(u *url.URL) (config, error) {
	res := config{
		Config: nsq.NewConfig(),
	}
	for k := range u.Query() {
		if err := res.Config.Set(k, u.Query().Get(k)); err != nil {
			return res, fmt.Errorf("setting option %s: %+v", k, err)
		}
	}
	res.Remotes = strings.Split(u.Host, ",")
	res.Topic = strings.Trim(u.Path, "/")
	res.Channel = u.Fragment
	return res, nil
}
