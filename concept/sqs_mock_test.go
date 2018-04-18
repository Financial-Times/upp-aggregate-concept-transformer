package concept

import (
	"errors"
	"sync"

	"github.com/Financial-Times/aggregate-concept-transformer/sqs"
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
)

type mockSQSClient struct {
	queue map[string]string
	s     sync.RWMutex
}

func (c *mockSQSClient) ListenAndServeQueue() []sqs.Notification {
	c.s.Lock()
	defer c.s.Unlock()
	q := c.queue
	notifications := []sqs.Notification{}
	for msgTag, UUID := range q {
		notifications = append(notifications, sqs.Notification{
			UUID:          UUID,
			ReceiptHandle: &msgTag,
		})
	}
	return notifications
}

func (c *mockSQSClient) RemoveMessageFromQueue(receiptHandle *string) error {
	c.s.Lock()
	defer c.s.Unlock()
	if _, ok := c.queue[*receiptHandle]; ok {
		delete(c.queue, *receiptHandle)
		return nil
	}
	return errors.New("Receipt handle not present on queue")
}

func (c *mockSQSClient) Queue() map[string]string {
	c.s.RLock()
	defer c.s.RUnlock()
	return c.queue
}

func (c *mockSQSClient) Healthcheck() fthealth.Check {
	return fthealth.Check{
		Checker: func() (string, error) {
			return "", nil
		},
	}
}
