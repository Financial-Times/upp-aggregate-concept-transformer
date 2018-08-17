package concordances

type ConcordanceRecord struct {
	UUID           string `json:"uuid"`
	Authority      string `json:"authority"`
	AuthorityValue string `json:"authorityValue"`
}
