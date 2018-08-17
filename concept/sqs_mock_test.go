package concept

import (
	"errors"
	"sync"

	"github.com/Financial-Times/aggregate-concept-transformer/sqs"
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
)

type mockSQSClient struct {
	conceptsQueue map[string]string
	eventList     []sqs.Event
	s             sync.RWMutex
	err           error
}

func (c *mockSQSClient) ListenAndServeQueue() []sqs.ConceptUpdate {
	c.s.Lock()
	defer c.s.Unlock()
	q := c.conceptsQueue
	notifications := []sqs.ConceptUpdate{}
	for msgTag, UUID := range q {
		notifications = append(notifications, sqs.ConceptUpdate{
			UUID:          UUID,
			ReceiptHandle: &msgTag,
		})
	}
	return notifications
}

func (c *mockSQSClient) RemoveMessageFromQueue(receiptHandle *string) error {
	c.s.Lock()
	defer c.s.Unlock()
	if _, ok := c.conceptsQueue[*receiptHandle]; ok {
		delete(c.conceptsQueue, *receiptHandle)
		return nil
	}
	return errors.New("Receipt handle not present on conceptsQueue")
}

func (c *mockSQSClient) SendEvents(messages []sqs.Event) error {
	if c.err != nil {
		return c.err
	}
	for _, event := range messages {
		c.eventList = append(c.eventList, event)
	}
	return nil
}

func (c *mockSQSClient) Queue() map[string]string {
	c.s.RLock()
	defer c.s.RUnlock()
	return c.conceptsQueue
}

func (c *mockSQSClient) Healthcheck() fthealth.Check {
	return fthealth.Check{
		Checker: func() (string, error) {
			return "", nil
		},
	}
}
