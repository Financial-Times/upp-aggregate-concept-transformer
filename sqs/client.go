package sqs

import (
	"encoding/json"
	"regexp"
	"strings"

	"fmt"
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/go-logger"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"strconv"
)

var keyMatcher = regexp.MustCompile("^[0-9a-f]{8}/[0-9a-f]{4}/[0-9a-f]{4}/[0-9a-f]{4}/[0-9a-f]{12}$")

type Client interface {
	ListenAndServeQueue() []ConceptUpdate
	SendEvents(messages []Event) error
	RemoveMessageFromQueue(receiptHandle *string) error
	Healthcheck() fthealth.Check
}

type NotificationClient struct {
	sqs          *sqs.SQS
	listenParams sqs.ReceiveMessageInput
	queueUrl     string
}

func NewClient(awsRegion string, queueUrl string, messagesToProcess int, visibilityTimeout int, waitTime int) (Client, error) {
	if queueUrl == "" {
		return &NotificationClient{
			queueUrl: queueUrl,
		}, nil
	}

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
		logger.WithError(err).Error("Unable to create an SQS client")
		return &NotificationClient{}, err
	}
	client := sqs.New(sess)
	return &NotificationClient{
		sqs:          client,
		listenParams: listenParams,
		queueUrl:     queueUrl,
	}, err
}

func (c *NotificationClient) ListenAndServeQueue() []ConceptUpdate {
	messages, err := c.sqs.ReceiveMessage(&c.listenParams)
	if err != nil {
		logger.WithError(err).Error("Error whilst listening for messages")
	}
	return getNotificationsFromMessages(messages.Messages)
}

func (c *NotificationClient) SendEvents(messages []Event) error {
	if !strings.Contains(c.queueUrl,"upp-concept-events-dev"){
		return nil
	}
	var entries []*sqs.SendMessageBatchRequestEntry

	for i, msg := range messages {

		jsonBytes, _ := json.Marshal(msg)

		entries = append(entries, &sqs.SendMessageBatchRequestEntry{
			MessageBody: aws.String(string(jsonBytes)),
			Id:          aws.String(string(msg.ConceptUUID + "_" + strconv.Itoa(i))),
		})
	}

	input := &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(c.queueUrl),
		Entries:  entries,
	}

	output, err := c.sqs.SendMessageBatch(input)
	if err != nil {
		if _, ok := err.(awserr.Error); ok {
			// We've got an AWS error, so handle accordingly.
			logger.WithError(err.(awserr.Error).OrigErr()).Errorf("SQS send error: %s", err.(awserr.Error).Message())
			return err.(awserr.Error).OrigErr()
		}
		return err
	}

	for _, v := range output.Failed {
		logger.WithError(fmt.Errorf("SQS Error Code %d", v.Code)).Error(*v.Message)
	}
	return nil
}

func (c *NotificationClient) RemoveMessageFromQueue(receiptHandle *string) error {
	deleteParams := sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueUrl),
		ReceiptHandle: receiptHandle,
	}
	if _, err := c.sqs.DeleteMessage(&deleteParams); err != nil {
		logger.WithError(err).Error("Error deleting message from SQS")
		return err
	}
	return nil
}

func getNotificationsFromMessages(messages []*sqs.Message) []ConceptUpdate {

	notifications := []ConceptUpdate{}

	for _, message := range messages {
		var err error
		receiptHandle := message.ReceiptHandle
		messageBody := Body{}
		if err = json.Unmarshal([]byte(*message.Body), &messageBody); err != nil {
			logger.WithError(err).Error("Failed to unmarshal SQS message")
			continue
		}

		msgRecord := Message{}
		if err = json.Unmarshal([]byte(messageBody.Message), &msgRecord); err != nil {
			logger.WithError(err).Error("Failed to unmarshal S3 notification")
			continue
		}

		if msgRecord.Records == nil {
			logger.Error("Cannot map message to expected JSON format - skipping")
			continue
		}
		key := msgRecord.Records[0].S3.Object.Key
		if keyMatcher.MatchString(key) != true {
			logger.WithField("key", key).Error("Key in message is not a valid UUID")
			continue
		}

		notifications = append(notifications, ConceptUpdate{
			UUID:          strings.Replace(key, "/", "-", 4),
			ReceiptHandle: receiptHandle,
		})
	}

	return notifications
}

func (c *NotificationClient) Healthcheck() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Editorial updates of concepts will not be written into UPP",
		Name:             "Check connectivity to SQS queue",
		PanicGuide:       "https://dewey.ft.com/aggregate-concept-transformer.html",
		Severity:         3,
		TechnicalSummary: `Cannot connect to SQS queue. If this check fails, check that Amazon SQS is available`,
		Checker: func() (string, error) {
			params := &sqs.GetQueueAttributesInput{
				QueueUrl:       aws.String(c.queueUrl),
				AttributeNames: []*string{aws.String("ApproximateNumberOfMessages")},
			}
			if _, err := c.sqs.GetQueueAttributes(params); err != nil {
				logger.WithError(err).Error("Got error running SQS health check")
				return "", err
			}
			return "", nil
		},
	}
}
