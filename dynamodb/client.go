package dynamodb

import (
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type Client interface {
	GetConcordance(uuid string) (ConceptConcordance, error)
}

type DynamoClient struct {
	table string
	svc   *dynamodb.DynamoDB
}

func NewClient(table string) (Client, error) {
	sess := session.Must(session.NewSession())
	svc := dynamodb.New(sess)

	return &DynamoClient{
		table: table,
		svc:   svc,
	}, nil
}

func (c *DynamoClient) GetConcordance(uuid string) (ConceptConcordance, error) {

	input := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"conceptId": {
				S: aws.String(uuid),
			},
		},
		TableName: aws.String(c.table),
	}

	result, err := c.svc.GetItem(input)
	if err != nil {
		log.WithError(err.(awserr.Error)).Error("Error response from DynamoDB")
		return ConceptConcordance{}, err
	}

	if result.Item == nil {
		// We don't find a concordance, but that's probably fine.  We'll return an record without concordances.
		log.WithField("UUID", uuid).Info("No concordance record found")
		return ConceptConcordance{UUID: uuid, ConcordedIds: []string{}}, nil
	}

	var concordance ConceptConcordance
	err = dynamodbattribute.UnmarshalMap(result.Item, &concordance)

	return concordance, nil
}
