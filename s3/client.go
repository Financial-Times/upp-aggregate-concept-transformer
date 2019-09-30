package s3

import (
	"context"
	"net"
	"net/http"
	"strings"
	"time"

	"encoding/json"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/go-logger"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Client interface {
	GetConceptAndTransactionID(ctx context.Context, UUID string) (bool, Concept, string, error)
	Healthcheck() fthealth.Check
}

type ConceptClient struct {
	s3         *s3.S3
	bucketName string
}

func NewClient(bucketName string, awsRegion string) (Client, error) {
	hc := http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          20,
			IdleConnTimeout:       90 * time.Second,
			MaxIdleConnsPerHost:   20,
			TLSHandshakeTimeout:   3 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
	sess, err := session.NewSession(
		&aws.Config{
			Region:     aws.String(awsRegion),
			MaxRetries: aws.Int(1),
			HTTPClient: &hc,
		})
	if err != nil {
		logger.WithError(err).Error("Unable to create an S3 client")
		return &ConceptClient{}, err
	}
	client := s3.New(sess)

	return &ConceptClient{
		s3:         client,
		bucketName: bucketName,
	}, err
}

func (c *ConceptClient) GetConceptAndTransactionID(ctx context.Context, UUID string) (bool, Concept, string, error) {
	getObjectParams := &s3.GetObjectInput{
		Bucket: aws.String(c.bucketName),
		Key:    aws.String(getKey(UUID)),
	}

	resp, err := c.s3.GetObjectWithContext(ctx, getObjectParams)
	if err != nil {
		e, ok := err.(awserr.Error)
		if ok && e.Code() == "NoSuchKey" {
			// NotFound rather than error, so no logging needed.
			return false, Concept{}, "", nil
		}
		logger.WithError(err).WithUUID(UUID).Error("Error retrieving concept from S3")
		return false, Concept{}, "", err
	}

	getHeadersParams := &s3.HeadObjectInput{
		Bucket: aws.String(c.bucketName),
		Key:    aws.String(getKey(UUID)),
	}
	ho, err := c.s3.HeadObjectWithContext(ctx, getHeadersParams)
	if err != nil {
		logger.WithError(err).WithUUID(UUID).Error("Cannot access S3 head object")
		return false, Concept{}, "", err
	}
	tid := ho.Metadata["Transaction_id"]

	var concept Concept
	if err = json.NewDecoder(resp.Body).Decode(&concept); err != nil {
		logger.WithError(err).WithUUID(UUID).Error("Cannot unmarshal object into a concept")
		return true, Concept{}, "", err
	}
	return true, concept, *tid, nil
}

func (c *ConceptClient) Healthcheck() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Editorial updates of concepts will not be written into UPP",
		Name:             "Check connectivity to S3 bucket",
		PanicGuide:       "https://dewey.ft.com/aggregate-concept-transformer.html",
		Severity:         3,
		TechnicalSummary: `Cannot connect to S3 bucket. If this check fails, check that Amazon S3 is available`,
		Checker: func() (string, error) {
			params := &s3.HeadBucketInput{
				Bucket: aws.String(c.bucketName), // Required
			}
			_, err := c.s3.HeadBucket(params)
			if err != nil {
				logger.WithError(err).Error("Got error running S3 health check")
				return "", err
			}
			return "", err
		},
	}

}

func getKey(UUID string) string {
	return strings.Replace(UUID, "-", "/", -1)
}
