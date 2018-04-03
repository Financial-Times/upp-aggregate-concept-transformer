package s3

import "time"

type MembershipRole struct {
	RoleUUID        string     `json:"roleUUID,omitempty"`
	InceptionDate   *time.Time `json:"inceptionDate,omitempty"`
	TerminationDate *time.Time `json:"terminationDate,omitempty"`
}

type Concept struct {
	UUID           string   `json:"uuid,omitempty"`
	PrefLabel      string   `json:"prefLabel,omitempty"`
	Authority      string   `json:"authority,omitempty"`
	AuthValue      string   `json:"authorityValue,omitempty"`
	Aliases        []string `json:"aliases,omitempty"`
	ParentUUIDs    []string `json:"parentUUIDs,omitempty"`
	BroaderUUIDs   []string `json:"broaderUUIDs,omitempty"`
	RelatedUUIDs   []string `json:"relatedUUIDs,omitempty"`
	Strapline      string   `json:"strapline,omitempty"`
	DescriptionXML string   `json:"descriptionXML,omitempty"`
	ImageURL       string   `json:"_imageUrl,omitempty"`
	Type           string   `json:"type,omitempty"`
	EmailAddress   string   `json:"emailAddress,omitempty"`
	FacebookPage   string   `json:"facebookPage,omitempty"`
	TwitterHandle  string   `json:"twitterHandle,omitempty"`
	ScopeNote      string   `json:"scopeNote,omitempty"`
	ShortLabel     string   `json:"shortLabel,omitempty"`

	InceptionDate   *time.Time `json:"inceptionDate,omitempty"`
	TerminationDate *time.Time `json:"terminationDate,omitempty"`
	FigiCode        string     `json:"figiCode,omitempty"`
	IssuedBy        string     `json:"issuedBy,omitempty"`

	IsAuthor bool `json:"isAuthor,omitempty"`

	MembershipRoles  []MembershipRole `json:"membershipRoles,omitempty"`
	OrganisationUUID string           `json:"organisationUUID,omitempty"`
	PersonUUID       string           `json:"personUUID,omitempty"`
}
