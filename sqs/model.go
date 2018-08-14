package sqs

type ConceptUpdate struct {
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

//Events
type ConceptChanges struct {
	ChangedRecords []Event  `json:"events"`
	UpdatedIds     []string `json:"updatedIDs"`
}

type Event struct {
	ConceptType  string      `json:"conceptType"`
	ConceptUUID  string      `json:"conceptUUID"`
	EventDetails interface{} `json:"eventDetails"`
}

type ConceptEvent struct {
	Type string `json:"type"`
}

type ConcordanceEvent struct {
	Type  string `json:"type"`
	OldID string `json:"oldID"`
	NewID string `json:"newID"`
}
