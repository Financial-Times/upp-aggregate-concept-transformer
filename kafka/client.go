package kafka

import (
	"github.com/Shopify/sarama"
)

type Client struct {
	Producer sarama.SyncProducer
	Topic    string
}

func NewClient(kafkaAddress string, topic string) (Client, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewSyncProducer([]string{kafkaAddress}, config)
	if err != nil {
		return Client{}, err
	}

	return Client{
		Producer: producer,
		Topic:    topic,
	}, nil
}
