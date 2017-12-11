package dynamodb

import (
	"errors"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/go-logger"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type Client interface {
	GetConcordance(uuid string) (ConceptConcordance, error)
	Healthcheck() fthealth.Check
}

type DynamoClient struct {
	table  string
	region string
	svc    *dynamodb.DynamoDB
}

func NewClient(region, table string) (Client, error) {
	sess := session.Must(session.NewSession())
	svc := dynamodb.New(sess, &aws.Config{
		Region: aws.String(region),
	})

	return &DynamoClient{
		table:  table,
		region: region,
		svc:    svc,
	}, nil
}

func (c *DynamoClient) GetConcordance(uuid string) (ConceptConcordance, error) {
	scanInput := &dynamodb.ScanInput{
		TableName:        aws.String(c.table),
		FilterExpression: aws.String("#conceptId = :x or contains(#concordedIds, :y)"),
		ExpressionAttributeNames: map[string]*string{
			"#conceptId":    aws.String("conceptId"),
			"#concordedIds": aws.String("concordedIds"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":x": {S: aws.String(uuid)},
			":y": {S: aws.String(uuid)},
		},
	}

	result, err := c.svc.Scan(scanInput)
	if err != nil {
		logger.WithError(err).WithField("UUID", uuid).Error("Error scanning DynamoDB for concordance record")
		return ConceptConcordance{}, err
	}

	if int(*result.Count) == 0 {
		// No concordance found, so we'll create a fake record to return the solo concept.
		logger.WithError(err).WithField("UUID", uuid).Debug("No matching record in db")
		return ConceptConcordance{UUID: uuid, ConcordedIds: []string{}}, nil
	}
	if int(*result.Count) > 1 {
		logger.WithField("UUID", uuid).Errorf("More than one concordance record found")
		return ConceptConcordance{}, errors.New("More than one concordance found.")
	}

	var concordance ConceptConcordance
	if err = dynamodbattribute.UnmarshalMap(result.Items[0], &concordance); err != nil {
		logger.WithError(err).WithUUID(uuid).Error("Unable to unmarshal concordance object")
		return ConceptConcordance{}, err
	}
	return concordance, nil
}

func (c *DynamoClient) Healthcheck() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Editorial updates of concepts will not be written into UPP",
		Name:             "Check connectivity to DynamoDB",
		PanicGuide:       "https://dewey.ft.com/aggregate-concept-transformer.html",
		Severity:         2,
		TechnicalSummary: `Cannot connect to DynamoDB. If this check fails, check that Amazon DynamoDB is available`,
		Checker: func() (string, error) {
			_, err := c.svc.DescribeTable(&dynamodb.DescribeTableInput{
				TableName: aws.String(c.table),
			})
			if err != nil {
				return "Cannot connect to DynamoDB", err
			}
			return "", nil
		},
	}
}
