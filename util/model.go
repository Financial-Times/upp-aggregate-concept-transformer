package service

type SourceRepresentations []SourceRepresentation

type ConcordedConceptJson struct {
	PrefLabel             string                `json:"prefLabel"`
	Type                  string                `json:"type"`
	UUID                  string                `json:"uuid"`
	SourceRepresentations SourceRepresentations `json:"sourceRepresentations"`
}

type SourceRepresentation struct {
	AuthValue string   `json:"authorityValue"`
	Authority string   `json:"authority"`
	PrefLabel string   `json:"prefLabel"`
	Type      string   `json:"type"`
	UUID      string   `json:"uuid"`
	Aliases   []string `json:"aliases"`
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
