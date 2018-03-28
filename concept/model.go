package concept

import (
	"time"

	"github.com/Financial-Times/aggregate-concept-transformer/s3"
)

type MembershipRole struct {
	RoleUUID        string     `json:"roleUUID,omitempty"`
	InceptionDate   *time.Time `json:"inceptionDate,omitempty"`
	TerminationDate *time.Time `json:"terminationDate,omitempty"`
}

type ConcordedConcept struct {
	PrefUUID       string   `json:"prefUUID,omitempty"`
	PrefLabel      string   `json:"prefLabel,omitempty"`
	Type           string   `json:"type,omitempty"`
	Aliases        []string `json:"aliases,omitempty"`
	Strapline      string   `json:"strapline,omitempty"`
	DescriptionXML string   `json:"descriptionXML,omitempty"`
	ImageURL       string   `json:"_imageUrl,omitempty"`
	EmailAddress   string   `json:"emailAddress,omitempty"`
	FacebookPage   string   `json:"facebookPage,omitempty"`
	TwitterHandle  string   `json:"twitterHandle,omitempty"`
	ScopeNote      string   `json:"scopeNote,omitempty"`
	ShortLabel     string   `json:"shortLabel,omitempty"`
	ParentUUIDs    []string `json:"parentUUIDs,omitempty"`
	BroaderUUIDs   []string `json:"broaderUUIDs,omitempty"`
	RelatedUUIDs   []string `json:"relatedUUIDs,omitempty"`

	InceptionDate   *time.Time `json:"inceptionDate,omitempty"`
	TerminationDate *time.Time `json:"terminationDate,omitempty"`
	FigiCode        string     `json:"figiCode,omitempty"`
	IssuedBy        string     `json:"issuedBy,omitempty"`

	IsAuthor bool `json:"isAuthor,omitempty"`

	MembershipRoles  []MembershipRole `json:"membershipRoles,omitempty"`
	OrganisationUUID string           `json:"organisationUUID,omitempty"`
	PersonUUID       string           `json:"personUUID,omitempty"`
	RoleUUID         string           `json:"roleUUID,omitempty"`

	SourceRepresentations []s3.Concept `json:"sourceRepresentations,omitempty"`
}

type UpdatedConcepts struct {
	UpdatedIds []string `json:"UpdatedIds"`
}
