package kafka

import (
	"github.com/Shopify/sarama"
)

type Client struct {
	Producer sarama.SyncProducer
	Topic    string
}

func NewClient(kafkaAddress []string, topic string) (Client, error) {
	producer, err := sarama.NewSyncProducer(kafkaAddress, nil)
	if err != nil {
		return Client{}, err
	}

	return Client{
		Producer: producer,
		Topic:    topic,
	}, nil
}
