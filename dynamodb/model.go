package dynamodb

type ConceptConcordance struct {
	UUID         string   `json:"conceptId"`
	ConcordedIds []string `json:"concordedIds"`
}
