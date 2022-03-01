package rocketmq

import (
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
)

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
