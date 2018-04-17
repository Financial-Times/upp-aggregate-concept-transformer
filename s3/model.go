package s3

type MembershipRole struct {
	RoleUUID        string `json:"membershipRoleUUID,omitempty"`
	InceptionDate   string `json:"inceptionDate,omitempty"`
	TerminationDate string `json:"terminationDate,omitempty"`
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

	InceptionDate   string `json:"inceptionDate,omitempty"`
	TerminationDate string `json:"terminationDate,omitempty"`
	FigiCode        string `json:"figiCode,omitempty"`
	IssuedBy        string `json:"issuedBy,omitempty"`

	IsAuthor bool `json:"isAuthor,omitempty"`

	MembershipRoles  []MembershipRole `json:"membershipRoles,omitempty"`
	OrganisationUUID string           `json:"organisationUUID,omitempty"`
	PersonUUID       string           `json:"personUUID,omitempty"`

	ProperName             string   `json:"properName,omitempty"`
	ShortName              string   `json:"shortName,omitempty"`
	HiddenLabel            string   `json:"hiddenLabel,omitempty"`
	FormerNames            []string `json:"formerNames,omitempty"`
	CountryCode            string   `json:"countryCode,omitempty"`
	CountryOfIncorporation string   `json:"countryOfIncorporation,omitempty"`
	PostalCode             string   `json:"postalCode,omitempty"`
	YearFounded            int      `json:"yearFounded,omitempty"`
	LeiCode                string   `json:"leiCode,omitempty"`
}
