package natsstreaming

import (
	"context"

	"github.com/micro/go-micro/broker"
	"github.com/nats-io/go-nats-streaming"
)

type optionsKey struct{}

// Options accepts stan.Options
func Options(opts stan.Options) broker.Option {
	return setBrokerOption(optionsKey{}, opts)
}

type clientIDKey struct{}

func ClientID(clientID string) broker.Option {
	return setBrokerOption(clientIDKey{}, clientID)
}

type clusterIDKey struct{}

func ClusterID(clusterID string) broker.Option {
	return setBrokerOption(clusterIDKey{}, clusterID)
}

func setBrokerOption(k, v interface{}) broker.Option {
	return func(o *broker.Options) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, k, v)
	}
}
