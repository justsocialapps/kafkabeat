package beater

import (
	"testing"
	"github.com/Shopify/sarama"
)

func TestDecodeJson(t *testing.T) {
	ev := &sarama.ConsumerMessage{
		Key: []byte("1"),
		Value: []byte(`{"event":"data"}`),
	}

	res := decodeJson("kafkabeat", ev)
	if res["event"] != "data" {
		t.Fatal("json parse failed")
	}
}
