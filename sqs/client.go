package sqs

import (
	"encoding/json"
	"regexp"
	"strings"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	log "github.com/sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var keyMatcher = regexp.MustCompile("^[0-9a-f]{8}/[0-9a-f]{4}/[0-9a-f]{4}/[0-9a-f]{4}/[0-9a-f]{12}$")

type Client interface {
	ListenAndServeQueue() []Notification
	RemoveMessageFromQueue(receiptHandle *string) error
	Healthcheck() fthealth.Check
}

type NotificationClient struct {
	sqs          *sqs.SQS
	listenParams sqs.ReceiveMessageInput
	queueUrl     string
}

func NewClient(awsRegion string, queueUrl string, messagesToProcess int, visibilityTimeout int, waitTime int) (Client, error) {
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
		log.WithError(err).Error("Unable to create an SQS client")
		return &NotificationClient{}, err
	}
	client := sqs.New(sess)
	return &NotificationClient{
		sqs:          client,
		listenParams: listenParams,
		queueUrl:     queueUrl,
	}, err
}

func (c *NotificationClient) ListenAndServeQueue() []Notification {
	messages, err := c.sqs.ReceiveMessage(&c.listenParams)
	if err != nil {
		log.WithError(err).Error("Error whilst listening for messages")
	}
	return getNotificationsFromMessages(messages.Messages)
}

func (c *NotificationClient) RemoveMessageFromQueue(receiptHandle *string) error {
	deleteParams := sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueUrl),
		ReceiptHandle: receiptHandle,
	}
	if _, err := c.sqs.DeleteMessage(&deleteParams); err != nil {
		log.WithError(err).Error("Error deleting message from SQS")
		return err
	}
	return nil
}

func getNotificationsFromMessages(messages []*sqs.Message) []Notification {

	notifications := []Notification{}

	for _, message := range messages {
		receiptHandle := message.ReceiptHandle
		messageBody := Body{}
		err := json.Unmarshal([]byte(*message.Body), &messageBody)

		if err != nil {
			log.WithError(err).Warn("Failed to unmarshal SQS message - skipping")
			continue
		}

		msgRecord := Message{}
		err = json.Unmarshal([]byte(messageBody.Message), &msgRecord)
		if err != nil {
			log.WithError(err).Warn("Failed to unmarshal S3 notification - skipping")
			continue
		}

		if msgRecord.Records == nil {
			log.Warn("Cannot map message to expected JSON format - skipping")
			continue
		}
		key := msgRecord.Records[0].S3.Object.Key
		if keyMatcher.MatchString(key) != true {
			log.WithField("key", key).Warn("Key in message is not a valid UUID")
			continue
		}

		notifications = append(notifications, Notification{
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
		Severity:         2,
		TechnicalSummary: `Cannot connect to SQS queue. If this check fails, check that Amazon SQS is available`,
		Checker: func() (string, error) {
			params := &sqs.GetQueueAttributesInput{
				QueueUrl:       aws.String(c.queueUrl),
				AttributeNames: []*string{aws.String("ApproximateNumberOfMessages")},
			}
			_, err := c.sqs.GetQueueAttributes(params)

			if err != nil {
				log.Errorf("Got error running SQS health check, %v", err.Error())
				return "", err
			}
			return "", err
		},
	}
}
