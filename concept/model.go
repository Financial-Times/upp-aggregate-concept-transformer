package concept

import "github.com/Financial-Times/aggregate-concept-transformer/s3"

type ConcordedConcept struct {
	PrefUUID              string       `json:"prefUUID,omitempty"`
	PrefLabel             string       `json:"prefLabel,omitempty"`
	Type                  string       `json:"type,omitempty"`
	Aliases               []string     `json:"aliases,omitempty"`
	Strapline             string       `json:"strapline,omitempty"`
	DescriptionXML        string       `json:"descriptionXML,omitempty"`
	ImageURL              string       `json:"_imageUrl,omitempty"`
	ParentUUIDs           []string     `json:"parentUUIDs,omitempty"`
	SourceRepresentations []s3.Concept `json:"sourceRepresentations,omitempty"`
}
