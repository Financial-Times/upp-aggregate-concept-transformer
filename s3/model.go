package s3

type Concept struct {
	UUID           string   `json:"uuid,omitempty"`
	PrefLabel      string   `json:"prefLabel,omitempty"`
	Authority      string   `json:"authority,omitempty"`
	AuthValue      string   `json:"authorityValue,omitempty"`
	Aliases        []string `json:"aliases,omitempty"`
	ParentUUIDs    []string `json:"parentUUIDs,omitempty"`
	Strapline      string   `json:"strapline,omitempty"`
	DescriptionXML string   `json:"descriptionXML,omitempty"`
	ImageURL       string   `json:"_imageUrl,omitempty"`
	Type           string   `json:"type,omitempty"`
}
