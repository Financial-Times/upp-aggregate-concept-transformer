package s3

import (
	"net"
	"net/http"
	"strings"
	"time"

	"encoding/json"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Client interface {
	GetConceptAndTransactionId(UUID string) (bool, Concept, string, error)
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
		return &ConceptClient{}, err
	}
	client := s3.New(sess)

	return &ConceptClient{
		s3:         client,
		bucketName: bucketName,
	}, err
}

func (c *ConceptClient) GetConceptAndTransactionId(UUID string) (bool, Concept, string, error) {
	getObjectParams := &s3.GetObjectInput{
		Bucket: aws.String(c.bucketName),
		Key:    aws.String(getKey(UUID)),
	}

	resp, err := c.s3.GetObject(getObjectParams)
	if err != nil {
		e, ok := err.(awserr.Error)
		if ok && e.Code() == "NoSuchKey" {
			return false, Concept{}, "", nil
		}
		return false, Concept{}, "", err
	}

	getHeadersParams := &s3.HeadObjectInput{
		Bucket: aws.String(c.bucketName),
		Key:    aws.String(getKey(UUID)),
	}
	ho, err := c.s3.HeadObject(getHeadersParams)
	if err != nil {
		log.Error("Cannot access s3 data")
		return false, Concept{}, "", err
	}
	tid := ho.Metadata["Transaction_id"]

	var concept Concept
	err = json.NewDecoder(resp.Body).Decode(&concept)

	return true, concept, *tid, err
}

func (c *ConceptClient) Healthcheck() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Editorial updates of concepts will not be written into UPP",
		Name:             "Check connectivity to S3 bucket",
		PanicGuide:       "https://dewey.ft.com/aggregate-concept-transformer.html",
		Severity:         2,
		TechnicalSummary: `Cannot connect to S3 bucket. If this check fails, check that Amazon S3 is available`,
		Checker: func() (string, error) {
			params := &s3.HeadBucketInput{
				Bucket: aws.String(c.bucketName), // Required
			}
			_, err := c.s3.HeadBucket(params)
			if err != nil {
				log.WithError(err).Error("Got error running S3 health check")
				return "", err
			}
			return "", err
		},
	}

}

func getKey(UUID string) string {
	return strings.Replace(UUID, "-", "/", -1)
}
