package kinesis

import (
	"encoding/json"
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type Client interface {
	AddRecordToStream(updatedConcept []byte, conceptType string) error
	Healthcheck() fthealth.Check
}

type KinesisClient struct {
	streamName string
	svc        *kinesis.Kinesis
}

func NewClient(streamName string, region string) (Client, error) {
	sess := session.Must(session.NewSession())
	svc := kinesis.New(sess, &aws.Config{
		Region: aws.String(region),
	})

	return &KinesisClient{
		streamName: streamName,
		svc:        svc,
	}, nil
}

func (c *KinesisClient) AddRecordToStream(updatedConcept []byte, conceptType string) error {
	putRecordInput := &kinesis.PutRecordInput{
		Data:         updatedConcept,
		StreamName:   aws.String(c.streamName),
		PartitionKey: aws.String(conceptType),
	}

	if _, err := c.svc.PutRecord(putRecordInput); err != nil {
		return err
	}
	return nil
}

func (c *KinesisClient) Healthcheck() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Editorial updates of concepts will not be written into UPP",
		Name:             "Check connectivity to Kinesis stream",
		PanicGuide:       "https://dewey.ft.com/aggregate-concept-transformer.html",
		Severity:         2,
		TechnicalSummary: `Cannot connect to Kinesis stream. If this check fails, check that Amazon Kinesis is available`,
		Checker: func() (string, error) {
			_, err := c.svc.DescribeStream(&kinesis.DescribeStreamInput{
				StreamName: aws.String(c.streamName),
			})
			if err != nil {
				return "Cannot connect to Kinesis stream", err
			}
			return "", nil
		},
	}
}
