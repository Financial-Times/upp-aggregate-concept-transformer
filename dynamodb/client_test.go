package dynamodb

import (
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

const (
	UUID         = "4f50b156-6c50-4693-b835-02f70d3f3bc0"
	DDB_TABLE    = "concordance"
	AWS_REGION   = "eu-west-1"
	DDB_ENDPOINT = "http://localhost:8000"
)

var db *dynamodb.DynamoDB
var DescribeTableParams = &dynamodb.DescribeTableInput{TableName: aws.String(DDB_TABLE)}

func init() {
	db = connectToLocalDB()
	createTableIfNotExists()
	addRecordToTable()
}

func TestDynamoClient_GetConcordance(t *testing.T) {
	c := newTestClient()
	concordance, err := c.GetConcordance(UUID)
	assert.NoError(t, err)
	assert.Equal(t, UUID, concordance.UUID)
	assert.Equal(t, []string{"6a30400b-6906-41f2-9b1f-93e4cfc60515", "942cefc6-eab1-439a-82e8-59b5c62204e6"}, concordance.ConcordedIds)
}

func TestDynamoClient_GetConcordance_NoRecords(t *testing.T) {
	c := newTestClient()
	concordance, err := c.GetConcordance("729a4bf5-4612-4d81-8bc5-707a0c4d1251")
	assert.NoError(t, err)
	assert.Equal(t, "729a4bf5-4612-4d81-8bc5-707a0c4d1251", concordance.UUID)
	assert.Equal(t, []string{}, concordance.ConcordedIds)
}

func TestDynamoClient_GetConcordance_TooManyRecords(t *testing.T) {
	c := newTestClient()
	_, err := c.GetConcordance("205e8dc8-a10a-45d8-9218-97f361311486")
	assert.Error(t, err)
	assert.Equal(t, "More than one concordance found.", err.Error())
}

func TestDynamoClient_Healthcheck(t *testing.T) {
	c := newTestClient()
	s, err := c.Healthcheck().Checker()
	assert.NoError(t, err)
	assert.Equal(t, "", s)
}

func newTestClient() Client {
	return &DynamoClient{
		table:  DDB_TABLE,
		region: AWS_REGION,
		svc:    db,
	}
}

func connectToLocalDB() *dynamodb.DynamoDB {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(AWS_REGION),
		Endpoint:    aws.String(DDB_ENDPOINT),
		Credentials: credentials.NewStaticCredentials("id", "secret", "token"),
	})
	if err != nil {
		log.WithError(err).Fatal("Failed to connect to DynamoDB.")
	}
	ddb := dynamodb.New(sess)
	return ddb
}

func createTableIfNotExists() error {
	_, err := db.DescribeTable(DescribeTableParams)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			if awsErr.Code() == dynamodb.ErrCodeResourceNotFoundException {
				params := &dynamodb.CreateTableInput{
					AttributeDefinitions: []*dynamodb.AttributeDefinition{
						{
							AttributeName: aws.String("conceptId"),
							AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
						},
					},
					KeySchema: []*dynamodb.KeySchemaElement{ // Required
						{
							AttributeName: aws.String("conceptId"),
							KeyType:       aws.String(dynamodb.KeyTypeHash),
						},
					},
					ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
						ReadCapacityUnits:  aws.Int64(5),
						WriteCapacityUnits: aws.Int64(5),
					},
					TableName: aws.String(DDB_TABLE),
				}
				if _, err := db.CreateTable(params); err != nil {
					log.WithError(err).Fatal("Failed to create table.")
				}
			}
		} else {
			log.WithError(err).Fatal("Failed to connect to local DynamoDB.")
			return err
		}

	}
	return err
}

func addRecordToTable() {
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			DDB_TABLE: {
				{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"conceptId": {
								S: aws.String("4f50b156-6c50-4693-b835-02f70d3f3bc0"),
							},
							"concordedIds": {
								SS: aws.StringSlice([]string{"942cefc6-eab1-439a-82e8-59b5c62204e6", "6a30400b-6906-41f2-9b1f-93e4cfc60515"}),
							},
						},
					},
				},
				{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"conceptId": {
								S: aws.String("34ab385c-621c-467e-b4f5-ce766559edef"),
							},
							"concordedIds": {
								SS: aws.StringSlice([]string{"205e8dc8-a10a-45d8-9218-97f361311486"}),
							},
						},
					},
				},
				{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"conceptId": {
								S: aws.String("205e8dc8-a10a-45d8-9218-97f361311486"),
							},
							"concordedIds": {
								SS: aws.StringSlice([]string{"19a9b7bc-04a5-49eb-a8c4-8e84551f396f"}),
							},
						},
					},
				},
			},
		},
	}
	if _, err := db.BatchWriteItem(input); err != nil {
		log.WithError(err).Fatal("Can't write data to database.")
	}

}
