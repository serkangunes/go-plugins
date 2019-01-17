// Package natsstreaming provides a NATS Streaming broker
package natsstreaming

import (
	"context"
	"strings"

	"github.com/micro/go-log"
	"github.com/micro/go-micro/broker"
	"github.com/micro/go-micro/cmd"
	"github.com/micro/go-micro/codec/json"
	"github.com/nats-io/go-nats-streaming"
)

type nsbroker struct {
	addrs     []string
	conn      stan.Conn
	opts      broker.Options
	nsopts    stan.Options
	clientID  string
	clusterID string
}

type subscriber struct {
	s       stan.Subscription
	opts    broker.SubscribeOptions
	subject string
}

type publication struct {
	t string
	m *broker.Message
}

func init() {
	cmd.DefaultBrokers["nats-streaming"] = NewBroker
}

func (n *publication) Topic() string {
	return n.t
}

func (n *publication) Message() *broker.Message {
	return n.m
}

func (n *publication) Ack() error {
	return nil
}

func (n *subscriber) Options() broker.SubscribeOptions {
	return n.opts
}

func (n *subscriber) Topic() string {
	return n.subject
}

func (n *subscriber) Unsubscribe() error {
	return n.s.Unsubscribe()
}

func (n *nsbroker) Address() string {
	if len(n.addrs) > 0 {
		return n.addrs[0]
	}

	return ""
}

func setAddrs(addrs []string) []string {
	var cAddrs []string
	for _, addr := range addrs {
		if len(addr) == 0 {
			continue
		}
		if !strings.HasPrefix(addr, "nats://") {
			addr = "nats://" + addr
		}
		cAddrs = append(cAddrs, addr)
	}
	if len(cAddrs) == 0 {
		cAddrs = []string{stan.DefaultNatsURL}
	}
	return cAddrs
}

func (n *nsbroker) Connect() error {
	if n.conn != nil {
		return nil
	}

	sc, err := stan.Connect(n.clusterID, n.clientID, stan.NatsURL(n.addrs[0]))

	if err != nil {
		return err
	}
	n.conn = sc
	return nil
}

func (n *nsbroker) Disconnect() error {
	n.conn.Close()
	return nil
}

func (n *nsbroker) Init(opts ...broker.Option) error {
	for _, o := range opts {
		o(&n.opts)
	}
	n.addrs = setAddrs(n.opts.Addrs)
	return nil
}

func (n *nsbroker) Options() broker.Options {
	return n.opts
}

func (n *nsbroker) Publish(topic string, msg *broker.Message, opts ...broker.PublishOption) error {
	b, err := n.opts.Codec.Marshal(msg)
	if err != nil {
		return err
	}
	return n.conn.Publish(topic, b)
}

func (n *nsbroker) Subscribe(topic string, handler broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	opt := broker.SubscribeOptions{
		AutoAck: true,
	}

	for _, o := range opts {
		o(&opt)
	}

	fn := func(msg *stan.Msg) {
		var m broker.Message
		if err := n.opts.Codec.Unmarshal(msg.Data, &m); err != nil {
			return
		}
		handler(&publication{m: &m, t: msg.Subject})
	}

	var sub stan.Subscription
	var err error

	if len(opt.Queue) > 0 {
		sub, err = n.conn.QueueSubscribe(topic, opt.Queue, fn)
	} else {
		sub, err = n.conn.Subscribe(topic, fn)
	}
	if err != nil {
		return nil, err
	}
	return &subscriber{s: sub, opts: opt, subject: topic}, nil
}

func (n *nsbroker) String() string {
	return "nats-streaming"
}

//NewBroker returns a new broker
func NewBroker(opts ...broker.Option) broker.Broker {
	options := broker.Options{
		// Default codec
		Codec:   json.Marshaler{},
		Context: context.Background(),
	}

	for _, o := range opts {
		o(&options)
	}

	var DefaultOptions = stan.Options{
		NatsURL:            stan.DefaultNatsURL,
		ConnectTimeout:     stan.DefaultConnectWait,
		AckTimeout:         stan.DefaultAckWait,
		DiscoverPrefix:     stan.DefaultDiscoverPrefix,
		MaxPubAcksInflight: stan.DefaultMaxPubAcksInflight,
		PingIterval:        stan.DefaultPingInterval,
		PingMaxOut:         stan.DefaultPingMaxOut,
	}

	natssOpts := DefaultOptions
	if n, ok := options.Context.Value(optionsKey{}).(stan.Options); ok {
		natssOpts = n
	}

	var clusterID string

	if c, ok := options.Context.Value(clusterIDKey{}).(string); ok {
		clusterID = c
	} else {
		log.Log("cluster ID not defined")
		return nil
	}

	var clientID string

	if c, ok := options.Context.Value(clientIDKey{}).(string); ok {
		clientID = c
	} else {
		log.Log("client ID not defined")
		return nil
	}

	nb := &nsbroker{
		opts:      options,
		nsopts:    natssOpts,
		addrs:     setAddrs(options.Addrs),
		clusterID: clusterID,
		clientID:  clientID,
	}

	return nb
}
