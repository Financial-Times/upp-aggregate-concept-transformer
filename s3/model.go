package s3

type MembershipRole struct {
	RoleUUID        string `json:"membershipRoleUUID,omitempty"`
	InceptionDate   string `json:"inceptionDate,omitempty"`
	TerminationDate string `json:"terminationDate,omitempty"`
}

type Concept struct {
	// Required fields
	UUID      string `json:"uuid,omitempty"`
	Type      string `json:"type,omitempty"`
	PrefLabel string `json:"prefLabel,omitempty"`
	Authority string `json:"authority,omitempty"`
	AuthValue string `json:"authorityValue,omitempty"`
	// Additional fields
	Aliases        []string `json:"aliases,omitempty"`
	ParentUUIDs    []string `json:"parentUUIDs,omitempty"`
	BroaderUUIDs   []string `json:"broaderUUIDs,omitempty"`
	RelatedUUIDs   []string `json:"relatedUUIDs,omitempty"`
	DescriptionXML string   `json:"descriptionXML,omitempty"`
	ImageURL       string   `json:"_imageUrl,omitempty"`
	EmailAddress   string   `json:"emailAddress,omitempty"`
	FacebookPage   string   `json:"facebookPage,omitempty"`
	TwitterHandle  string   `json:"twitterHandle,omitempty"`
	ScopeNote      string   `json:"scopeNote,omitempty"`
	ShortLabel     string   `json:"shortLabel,omitempty"`
	// Brand
	Strapline string `json:"strapline,omitempty"`
	// Person
	IsAuthor bool `json:"isAuthor,omitempty"`
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
	HiddenLabel            string   `json:"hiddenLabel,omitempty"`
	LeiCode                string   `json:"leiCode,omitempty"`
	ParentOrganisation     string   `json:"parentOrganisation,omitempty"`
	PostalCode             string   `json:"postalCode,omitempty"`
	ProperName             string   `json:"properName,omitempty"`
	ShortName              string   `json:"shortName,omitempty"`
	YearFounded            int      `json:"yearFounded,omitempty"`
	IsDeprecated           bool     `json:"isDeprecated,omitempty"`
}
