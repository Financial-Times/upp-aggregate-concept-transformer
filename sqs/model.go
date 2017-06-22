package sqs

type Notification struct {
	UUID          string
	ReceiptHandle *string
}

//SQS Message Format
type Body struct {
	Message string `json:"Message"`
}

type Message struct {
	Records []Record `json:"Records"`
}

type Record struct {
	S3 s3 `json:"s3"`
}

type s3 struct {
	Object object `json:"object"`
}

type object struct {
	Key string `json:"key"`
}
