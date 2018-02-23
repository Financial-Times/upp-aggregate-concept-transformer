package kinesis

import (
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"fmt"
	"github.com/Financial-Times/go-logger"
	"github.com/pkg/errors"
)

type Client interface {
	AddRecordToStream(updatedConcept []byte, conceptType string) error
	Healthcheck() fthealth.Check
}

type KinesisClient struct {
	streamName string
	svc        *kinesis.Kinesis
}

func NewClient(streamName string, region string, arn string, environment string) (Client, error) {
	fmt.Println("Creating assume role provider")
	arp := &stscreds.AssumeRoleProvider{
		RoleARN: arn,
		RoleSessionName: fmt.Sprintf("UPP-aggregate-concept-transformer-%s", environment),
	}

	c, err := arp.Retrieve()
	if err != nil {
		logger.WithError(err).Error("Error assuming role provider")
		return &KinesisClient{}, err
	} else if c.SessionToken == "" {
		logger.WithError(err).Error("Error reading session token")
		return &KinesisClient{}, errors.New("Session token was null")
	} else if c.AccessKeyID == "" {
		logger.WithError(err).Error("Error reading access key")
		return &KinesisClient{}, errors.New("access key was null")
	} else if c.SecretAccessKey == "" {
		logger.WithError(err).Error("Error reading secret key")
		return &KinesisClient{}, errors.New("Secret key was null")
	}
	sess := session.Must(session.NewSession())
	fmt.Println("Passing assume role provider to kinesis client")
	svc := kinesis.New(sess, &aws.Config{
		Region: aws.String(region),
		Credentials: credentials.NewCredentials(arp),
	})
	fmt.Println("Successfully created kinesis client")

	_, err = svc.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})
	if err != nil {
		logger.WithError(err).Error("Cannot connect to Kinesis stream")
		return &KinesisClient{}, err
	}

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
