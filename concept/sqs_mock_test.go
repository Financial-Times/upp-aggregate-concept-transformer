package concept

import (
	"context"
	"errors"
	"sync"

	"github.com/stretchr/testify/mock"

	"github.com/Financial-Times/aggregate-concept-transformer/sqs"
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
)

type mockSQSClient struct {
	mock.Mock
	conceptsQueue map[string]string
	eventList     []sqs.Event
	s             sync.RWMutex
	err           error
}

func (c *mockSQSClient) ListenAndServeQueue(ctx context.Context) []sqs.ConceptUpdate {
	c.s.Lock()
	defer c.s.Unlock()
	c.Called()
	q := c.conceptsQueue
	notifications := []sqs.ConceptUpdate{}
	for msgTag, UUID := range q {
		notifications = append(notifications, sqs.ConceptUpdate{
			UUID:          UUID,
			Bookmark:      "",
			ReceiptHandle: &msgTag,
		})
	}
	return notifications
}

func (c *mockSQSClient) RemoveMessageFromQueue(ctx context.Context, receiptHandle *string) error {
	c.s.Lock()
	defer c.s.Unlock()
	if _, ok := c.conceptsQueue[*receiptHandle]; ok {
		delete(c.conceptsQueue, *receiptHandle)
		return nil
	}
	return errors.New("Receipt handle not present on conceptsQueue")
}

func (c *mockSQSClient) SendEvents(ctx context.Context, messages []sqs.Event) error {
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
