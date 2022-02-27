package rocketmq

import (
	"context"
	"os"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/beats/v7/libbeat/publisher"
)

type rocketmqOutput struct {
	log      *logp.Logger
	out      *os.File
	observer outputs.Observer
	index    string
	codec    codec.Codec
	host     string
	topic    string
	//mq       *RocketMq
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
	out := &rocketmqOutput{log: logp.NewLogger("rocketmq"),
		out:      os.Stdout,
		observer: observer,
		index:    index,
		codec:    codec,
		host:     config.NamesrvAddr,
		topic:    config.Topic}

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
func (c *rocketmqOutput) Connect() error {
	c.log.Warn("Enter Connect()..............")

	return nil
}

//关闭触发函数
func (c *rocketmqOutput) Close() error {
	c.log.Warn("Enter Close()...............")
	// if c.mq != nil {
	// 	c.mq.Shutdown()
	// }
	return nil
}

//有新的日志信息产生，会触发该函数
func (c *rocketmqOutput) Publish(_ context.Context, batch publisher.Batch) error {
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

	// st := c.observer
	// events := batch.Events()
	// st.NewBatch(len(events))

	// dropped := 0
	// for i := range events {
	// 	ok := c.publishEvent(&events[i])
	// 	if !ok {
	// 		dropped++
	// 	}
	// }

	// batch.ACK()

	// st.Dropped(dropped)
	// st.Acked(len(events) - dropped)

	return nil
}

func (c *rocketmqOutput) publishEvent(event *publisher.Event) bool {
	c.log.Warn("Enter publishEvent().......................")

	serializedEvent, err := c.codec.Encode(c.index, &event.Content)
	if err != nil {
		if !event.Guaranteed() {
			return false
		}
		c.log.Errorf("Unable to encode event: %+v", err)
		c.log.Debugf("Failed event: %v", event)
		return false
	}

	c.observer.WriteBytes(len(serializedEvent) + 1)

	//判断生产者是否为空，如果为空重新初始化注册到 rocketmq 中
	// if c.mq.isShutdown() == true {
	// 	arr := strings.Split(c.host, ",")
	// 	c.mq = RegisterRocketProducerMust(arr, "logByFilebeat", 1)
	// }
	//新增日志内容
	str := string(serializedEvent)
	c.log.Warn("Processing event: %s", str)

	//发送内容到 rocketmq
	// msg, err := c.mq.SendMsg(c.topic, str)
	// c.log.Debug("msg:%v", msg)

	if err != nil {
		c.log.Errorf("send to rocketmq  is error %+v", err)
		return false
	}
	return true
}

//接口规范
func (c *rocketmqOutput) String() string {
	return "rocketmq"
}
