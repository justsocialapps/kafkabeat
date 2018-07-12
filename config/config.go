// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

type Config struct {
	Brokers []string `config:"brokers"`
	Topics  []string `config:"topics"`
	Group   string   `config:"group"`
	Offset  string   `config:"offset"`
	Codec   string   `config:"codec"`
}

var DefaultConfig = Config{
	Brokers: []string{"localhost:9092"},
	Topics:  []string{"tracking"},
	Group:   "kafkabeat",
	Offset:  "newest",
	Codec:   "plain",
}
