package rocketmq

import (
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
)

type rocketmqConfig struct {
	NamesrvAddr string       `config:"namesrv_addr"               validate:"required"`
	Topic       string       `config:"topic"`
	Codec       codec.Config `config:"codec"`
}

func defaultConfig() rocketmqConfig {
	return rocketmqConfig{
		NamesrvAddr: "localhost:9876",
		Topic:       "mytopic",
	}
}

func readConfig(cfg *common.Config) (*rocketmqConfig, error) {
	c := defaultConfig()
	if err := cfg.Unpack(&c); err != nil {
		return nil, err
	}
	return &c, nil
}
