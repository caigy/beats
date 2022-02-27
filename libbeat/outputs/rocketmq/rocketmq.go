package rocketmq

import (
	"context"

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
	namesrvAddr string
	topic       string

	//mq       *RocketMq
	producer rocketmq.Producer
}

const (
	logSelector = "rocketmq"
)

func init() { //初始化，把我们定义的 rocketmq struct 注册进来
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

	//创建 rocketmq struct， 传入配置文件的 rocketmq host 和 topic 进行保存。
	out := &client{log: log,
		observer:    observer,
		index:       index,
		codec:       codec,
		namesrvAddr: config.NamesrvAddr,
		topic:       config.Topic}

	//arr := strings.Split(config.Host, ",") //针对 rocketmq 可能集群配置 xxx:9876,xxxx:9876
	//rocketmq 的生产者开始注册，其中 group 写死了= logByFilebeat,重新
	//out.mq = RegisterRocketProducerMust(arr, "logByFilebeat", 1)

	// check stdout actually being available
	// if runtime.GOOS != "windows" {
	// 	if _, err = out.out.Stat(); err != nil {
	// 		err = fmt.Errorf("rocketmq output initialization failed with: %v", err)
	// 		return outputs.Fail(err)
	// 	}
	// }

	//没有大小限制=-1，不尝试重试=0
	return outputs.Success(-1, 0, out)
}

///////////////// in client.go //////////////////////////
func (c *client) Connect() error {
	c.log.Warn("Enter Connect()..............")
	c.log.Warnf("connect: %v", c.namesrvAddr)

	p, err := rocketmq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
		producer.WithRetry(2),
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

	batch.ACK()
	st.Dropped(dropped)
	st.Acked(len(events) - dropped)

	return nil
}

func (c *client) publishEvent(event *publisher.Event) bool {
	c.log.Warn("Enter publishEvent().......................")

	serializedEvent, err := c.codec.Encode(c.index, &event.Content)
	if err != nil {
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

	res, err := c.producer.SendSync(context.Background(), msg)

	if err != nil {
		c.log.Errorf("send to rocketmq  is error %+v", err)
		return false
	} else {
		c.log.Warn("send msg result=%v", res.String())
	}
	return true
}

//接口规范
func (c *client) String() string {
	return "rocketmq"
}
