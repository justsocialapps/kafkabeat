package beater

import (
	"fmt"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"

	"github.com/Shopify/sarama"
	"github.com/justsocialapps/kafkabeat/config"
	cluster "gopkg.in/bsm/sarama-cluster.v2"
)

type Kafkabeat struct {
	done     chan struct{}
	config   config.Config
	client   beat.Client
	consumer *cluster.Consumer
}

// Creates beater
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	config := config.DefaultConfig
	if err := cfg.Unpack(&config); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	kafkaConfig := cluster.NewConfig()
	kafkaConfig.Consumer.Return.Errors = true
	kafkaConfig.Group.Return.Notifications = true
	kafkaConfig.Config.ClientID = "kafkabeat"
	kafkaConfig.Config.Consumer.Offsets.Initial = sarama.OffsetOldest
	kafkaConfig.Config.Consumer.MaxWaitTime = 500 * time.Millisecond
	kafkaConfig.Config.Consumer.MaxProcessingTime = 5000 * time.Millisecond

	consumer, err := cluster.NewConsumer(config.Brokers, config.Group, config.Topics, kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("Error starting consumer on %s: %s", "tracking", err)
	}

	bt := &Kafkabeat{
		done:     make(chan struct{}),
		config:   config,
		consumer: consumer,
	}
	return bt, nil
}

func (bt *Kafkabeat) Run(b *beat.Beat) error {
	logp.Info("kafkabeat is running! Hit CTRL-C to stop it.")

	var err error
	bt.client, err = b.Publisher.ConnectWith(beat.ClientConfig{
		PublishMode: beat.GuaranteedSend,
	})
	if err != nil {
		logp.Err("Error connecting to logstash: %s", err.Error())
	}

	for {
		select {
		case <-bt.done:
			bt.consumer.Close()
			return nil
		case ev := <-bt.consumer.Messages():
			event := beat.Event{
				Timestamp: time.Now(),
				Fields: common.MapStr{
					"type":    b.Info.Name,
					"message": string(ev.Value),
				},
			}
			bt.client.Publish(event)
			bt.consumer.MarkOffset(ev, "")

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
