package beater

import (
	"fmt"
	"time"
	"encoding/json"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher/bc/publisher"

	"github.com/Shopify/sarama"
	"github.com/justsocialapps/kafkabeat/config"
	"gopkg.in/bsm/sarama-cluster.v2"
)

type (
	eventCodecFn func(t string, ev *sarama.ConsumerMessage) map[string]interface{}

	Kafkabeat struct {
		done     chan struct{}
		config   config.Config
		client   publisher.Client
		consumer *cluster.Consumer
		codec    eventCodecFn
	}
)

// Creates beater
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	btCfg := config.DefaultConfig
	if err := cfg.Unpack(&btCfg); err != nil {
		return nil, fmt.Errorf("error reading config file: %v", err)
	}

	kafkaConfig := cluster.NewConfig()
	kafkaConfig.Consumer.Return.Errors = true
	kafkaConfig.Group.Return.Notifications = true

	// offset handling
	switch btCfg.Offset {
	case "newest":
		kafkaConfig.Config.Consumer.Offsets.Initial = sarama.OffsetNewest
	case "oldest":
		kafkaConfig.Config.Consumer.Offsets.Initial = sarama.OffsetOldest
	default:
		return nil, fmt.Errorf("unknown offset: '%s'", btCfg.Offset)
	}

	// codec handling
	var codec eventCodecFn
	switch btCfg.Codec {
	case "plain":
		codec = decodePlain
	case "json":
		codec = decodeJson
	default:
		return nil, fmt.Errorf("unknown codec: '%s'", btCfg.Codec)
	}

	consumer, err := cluster.NewConsumer(btCfg.Brokers, btCfg.Group, btCfg.Topics, kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("error starting consumer on %s: %s", "tracking", err)
	}

	bt := &Kafkabeat{
		done:     make(chan struct{}),
		config:   btCfg,
		consumer: consumer,
		codec:    codec,
	}
	return bt, nil
}

func (bt *Kafkabeat) Run(b *beat.Beat) error {
	logp.Info("kafkabeat is running! Hit CTRL-C to stop it.")

	bt.client = b.Publisher.Connect()
	for {
		select {
		case <-bt.done:
			bt.consumer.Close()
			return nil

		case ev := <-bt.consumer.Messages():
			event := bt.codec(b.Info.Name, ev)
			if event == nil {
				logp.Err("Empty event decoded from: %#v, skipping..", string(ev.Value))
				bt.consumer.MarkOffset(ev, "")
				continue
			}

			if bt.client.PublishEvent(event, publisher.Guaranteed, publisher.Sync) {
				bt.consumer.MarkOffset(ev, "")
			}

		case notification := <-bt.consumer.Notifications():
			logp.Info("Rebalanced: %+v", notification)

		case err := <-bt.consumer.Errors():
			logp.Err("Error in Kafka consumer: %s", err.Error())
		}
	}
}

func (bt *Kafkabeat) Stop() {
	bt.client.Close()
	close(bt.done)
}

// decode plain message from kafka
func decodePlain(t string, ev *sarama.ConsumerMessage) map[string]interface{} {
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now()
	}

	event := common.MapStr{
		"@timestamp": common.Time(ev.Timestamp),
		"type":       t,
		"message":    string(ev.Value),
	}
	return event
}

// decode json event from kafka.
func decodeJson(t string, ev *sarama.ConsumerMessage) map[string]interface{} {
	event := map[string]interface{}{}

	err := json.Unmarshal(ev.Value, &event)
	if err != nil {
		logp.Err("Decode failed: %#v", err)
		return nil
	}
	if _, exists := event["type"]; !exists {
		event["type"] = t
	}
	if _, exists := event["@timestamp"]; !exists {
		if ev.Timestamp.IsZero() {
			ev.Timestamp = time.Now()
		}
		event["@timestamp"] = common.Time(ev.Timestamp)
	}

	return event
}
