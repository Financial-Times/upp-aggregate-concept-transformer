package kafka

import (
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
)

type Client struct {
	producer sarama.AsyncProducer
	topic    string
}

func NewClient(zkAddresses []string, topic string) (Client, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer(zkAddresses, config)
	if err != nil {
		return Client{}, err
	}

	go func() {
		for pm := range producer.Successes() {
			log.Infof("Successfully produced message on topic %s", pm.Topic)
		}
	}()

	go func() {
		for err := range producer.Errors() {
			log.Errorf("Error producing message: %s", err)
		}
	}()

	return Client{
		producer: producer,
		topic:    topic,
	}, nil
}

func (c *Client) SendMessage(message string) {

	m := &sarama.ProducerMessage{
		Topic: c.topic,
		Value: sarama.StringEncoder(message),
	}

	c.producer.Input() <- m
}
