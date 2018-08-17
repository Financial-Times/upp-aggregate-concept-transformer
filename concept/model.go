package concept

import (
	"github.com/Financial-Times/aggregate-concept-transformer/s3"
)

type MembershipRole struct {
	RoleUUID        string `json:"membershipRoleUUID,omitempty"`
	InceptionDate   string `json:"inceptionDate,omitempty"`
	TerminationDate string `json:"terminationDate,omitempty"`
}

type ConcordedConcept struct {
	// Required fields
	PrefUUID  string `json:"prefUUID,omitempty"`
	PrefLabel string `json:"prefLabel,omitempty"`
	Type      string `json:"type,omitempty"`
	// Additional fields
	Aliases        []string `json:"aliases,omitempty"`
	BroaderUUIDs   []string `json:"broaderUUIDs,omitempty"`
	DescriptionXML string   `json:"descriptionXML,omitempty"`
	EmailAddress   string   `json:"emailAddress,omitempty"`
	FacebookPage   string   `json:"facebookPage,omitempty"`
	ImageURL       string   `json:"_imageUrl,omitempty"`
	ParentUUIDs    []string `json:"parentUUIDs,omitempty"`
	RelatedUUIDs   []string `json:"relatedUUIDs,omitempty"`
	ScopeNote      string   `json:"scopeNote,omitempty"`
	ShortLabel     string   `json:"shortLabel,omitempty"`
	TwitterHandle  string   `json:"twitterHandle,omitempty"`
	// Brand
	Strapline string `json:"strapline,omitempty"`
	// Person
	IsAuthor   bool   `json:"isAuthor,omitempty"`
	Salutation string `json:"salutation,omitempty"`
	BirthYear  int    `json:"birthYear,omitempty"`
	// Financial Instrument
	FigiCode string `json:"figiCode,omitempty"`
	IssuedBy string `json:"issuedBy,omitempty"`
	// Membership
	InceptionDate    string           `json:"inceptionDate,omitempty"`
	MembershipRoles  []MembershipRole `json:"membershipRoles,omitempty"`
	OrganisationUUID string           `json:"organisationUUID,omitempty"`
	PersonUUID       string           `json:"personUUID,omitempty"`
	TerminationDate  string           `json:"terminationDate,omitempty"`
	// Organisation
	CountryCode            string   `json:"countryCode,omitempty"`
	CountryOfIncorporation string   `json:"countryOfIncorporation,omitempty"`
	FormerNames            []string `json:"formerNames,omitempty"`
	TradeNames             []string `json:"tradeNames,omitempty"`
	LeiCode                string   `json:"leiCode,omitempty"`
	PostalCode             string   `json:"postalCode,omitempty"`
	ProperName             string   `json:"properName,omitempty"`
	ShortName              string   `json:"shortName,omitempty"`
	YearFounded            int      `json:"yearFounded,omitempty"`
	IsDeprecated           bool     `json:"isDeprecated,omitempty"`
	// Source representations
	SourceRepresentations []s3.Concept `json:"sourceRepresentations,omitempty"`
}
