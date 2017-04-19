package sqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/coreos/fleet/log"
)

type SqsDriver interface {
	ListenAndServeQueue() []*sqs.Message
	RemoveMessageFromQueue(receiptHandle *string) error
	HealthCheck() (string, error)
}

type Client struct {
	sqs          *sqs.SQS
	listenParams sqs.ReceiveMessageInput
	queueUrl     string
}

func NewClient(awsRegion string, queueUrl string, messagesToProcess int, visibilityTimeout int, waitTime int) (SqsDriver, error) {
	listenParams := sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueUrl),
		MaxNumberOfMessages: aws.Int64(int64(messagesToProcess)),
		VisibilityTimeout:   aws.Int64(int64(visibilityTimeout)),
		WaitTimeSeconds:     aws.Int64(int64(waitTime)),
	}

	sess, err := session.NewSession(&aws.Config{
		Region:     aws.String(awsRegion),
		MaxRetries: aws.Int(3),
	})
	if err != nil {
		return &Client{}, err
	}
	client := sqs.New(sess)
	return &Client{
		sqs:          client,
		listenParams: listenParams,
		queueUrl:     queueUrl,
	}, err
}

func (c *Client) ListenAndServeQueue() []*sqs.Message {
	messages, err := c.sqs.ReceiveMessage(&c.listenParams)
	if err != nil {
		log.Errorf("Error whilst listening for messages: %v", err.Error())
	}
	m := messages.Messages
	return m
}

func (c *Client) RemoveMessageFromQueue(receiptHandle *string) error {
	deleteParams := sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueUrl),
		ReceiptHandle: receiptHandle,
	}
	_, err := c.sqs.DeleteMessage(&deleteParams)
	return err
}

func (c *Client) HealthCheck() (string, error) {
	params := &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(c.queueUrl),
		AttributeNames: []*string{aws.String("ApproximateNumberOfMessages")},
	}
	_, err := c.sqs.GetQueueAttributes(params)

	if err != nil {
		log.Errorf("Got error running SQS health check, %v", err.Error())
		return "Can not perform check on SQS availability", err
	}
	return "Access to SQS queue ok", err
}
