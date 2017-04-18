package service

type Concept struct {
	UUID                   string                 `json:"uuid"`
	PrefLabel              string                 `json:"prefLabel"`
	AlternativeIdentifiers alternativeIdentifiers `json:"alternativeIdentifiers"`
	Type                   string                 `json:"type,omitempty"`
}

type alternativeIdentifiers struct {
	TME   []string `json:"TME,omitempty"`
	UUIDS []string `json:"uuids"`
}

type SourceRepresentations []SourceRepresentation

type SourceConceptJson struct {
	Aliases                []string `json:"aliases"`
	AlternativeIdentifiers struct {
		TME   []string `json:"TME"`
		Uuids []string `json:"uuids"`
	} `json:"alternativeIdentifiers"`
	Name      string `json:"name"`
	PrefLabel string `json:"prefLabel"`
	Type      string `json:"type"`
	UUID      string `json:"uuid"`
}

type ConcordedConceptJson struct {
	PrefLabel             string                `json:"prefLabel"`
	Type                  string                `json:"type"`
	UUID                  string                `json:"uuid"`
	SourceRepresentations SourceRepresentations `json:"sourceRepresentations"`
}

type SourceRepresentation struct {
	AuthValue string `json:"authorityValue"`
	Authority string `json:"authority"`
	PrefLabel string `json:"prefLabel"`
	Type      string `json:"type"`
	UUID      string `json:"uuid"`
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
