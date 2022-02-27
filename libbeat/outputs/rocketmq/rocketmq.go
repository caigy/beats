package rocketmq

import (
	"context"
	"strings"
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/beats/v7/libbeat/publisher"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

type client struct {
	log         *logp.Logger
	observer    outputs.Observer
	index       string
	codec       codec.Codec
	namesrvAddr []string
	topic       string
	group       string
	timeout     time.Duration
	maxRetries  int

	producer rocketmq.Producer
}

const (
	logSelector = "rocketmq"
)

func init() {
	outputs.RegisterType("rocketmq", makeRocketmq)
}

func makeRocketmq(
	_ outputs.IndexManager,
	beat beat.Info,
	observer outputs.Observer,
	cfg *common.Config,
) (outputs.Group, error) {
	log := logp.NewLogger(logSelector)
	log.Warn("Enter makeRocketmq()!!!!!!")

	config, err := readConfig(cfg)
	if err != nil {
		return outputs.Fail(err)
	}

	index := beat.Beat

	codec, err := codec.CreateEncoder(beat, config.Codec)
	if err != nil {
		return outputs.Fail(err)
	}

	client := &client{log: log,
		observer:    observer,
		index:       index,
		codec:       codec,
		namesrvAddr: config.Nameservers,
		topic:       config.Topic,
		group:       config.Group,
		timeout:     config.SendTimeout,
		maxRetries:  config.MaxRetries}

	//retry in producer, so set it 0 in the following function
	return outputs.Success(config.BatchSize, 0, client)
}

///////////////// in client.go //////////////////////////
func (c *client) Connect() error {
	c.log.Warn("Enter Connect()..............")
	c.log.Warnf("connect: %v", c.namesrvAddr)

	p, err := rocketmq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver(c.namesrvAddr)),
		producer.WithRetry(c.maxRetries),
		producer.WithSendMsgTimeout(c.timeout),
		producer.WithGroupName(c.group),
	)
	if err != nil {
		c.log.Errorf("RocketMQ creates producer fails with: %v", err)
		return err
	}

	errStart := p.Start()
	if errStart != nil {
		c.log.Errorf("RocketMQ starts producer fails with: %v", errStart)
		return errStart
	}

	c.producer = p

	return nil
}

//关闭触发函数
func (c *client) Close() error {
	c.log.Warn("Enter Close()...............")
	if c.producer != nil {
		c.producer.Shutdown()
		c.producer = nil
	}

	return nil
}

//有新的日志信息产生，会触发该函数
func (c *client) Publish(_ context.Context, batch publisher.Batch) error {
	c.log.Warn("Enter Publish()..........")

	defer batch.ACK()
	st := c.observer
	events := batch.Events()
	st.NewBatch(len(events))

	dropped := 0
	for i := range events {
		d := &events[i]

		ok := c.publishEvent(d)
		if !ok {
			dropped++
		}
	}

	// batch.ACK()
	// st.Dropped(dropped)
	// st.Acked(len(events) - dropped)

	return nil
}

func (c *client) publishEvent(event *publisher.Event) bool {
	c.log.Warn("Enter publishEvent().......................")

	serializedEvent, err := c.codec.Encode(c.index, &event.Content)
	if err != nil {
		c.observer.Dropped(1)

		if !event.Guaranteed() {
			return false
		}
		c.log.Errorf("Unable to encode event: %v", err)
		c.log.Debugf("Failed event: %v", event)
		return false
	}

	c.observer.WriteBytes(len(serializedEvent) + 1)

	//新增日志内容
	str := string(serializedEvent)
	c.log.Warnf("Processing event: %v", str)

	buf := make([]byte, len(serializedEvent))
	copy(buf, serializedEvent)

	msg := &primitive.Message{
		Topic: c.topic,
		Body:  buf,
	}

	// res, err := c.producer.SendSync(context.Background(), msg)

	// if err != nil {
	// 	c.log.Errorf("send to rocketmq  is error %v", err)
	// 	return false
	// } else {
	// 	c.log.Warnf("send msg result=%v", res.String())
	// }

	err = c.producer.SendAsync(context.Background(),
		func(ctx context.Context, result *primitive.SendResult, e error) {
			if e != nil {
				c.observer.Dropped(1)
				c.log.Errorf("send to rocketmq is error %v", e)
			} else {
				c.observer.Acked(1)
				c.log.Warnf("send msg result=%v", result.String())
			}
		}, msg)

	if err != nil {
		c.observer.Dropped(1)
		c.log.Errorf("send to rocketmq is error %v", err)
		return false
	}

	return true
}

func (c *client) String() string {
	return "rocketmq[" + strings.Join(c.namesrvAddr, ",") + "]"
}
