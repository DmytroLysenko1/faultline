package credential

import (
	"fmt"
	"time"
)

type Grants map[ID]Grant

func (g Grants) Admit(token IngestToken, tenant string) (Grant, error) {
	grantOrDecoy, found := g.lookup(token.ID)
	secretMatches := grantOrDecoy.Digest.Equal(token.Digest())
	if !found {
		return Grant{}, ErrUnknownGrant
	}
	if !secretMatches {
		return Grant{}, ErrDigestMismatch
	}
	if grantOrDecoy.Tenant != tenant {
		return Grant{}, ErrForeignTenant
	}
	return grantOrDecoy, nil
}

func (g Grants) lookup(id ID) (Grant, bool) {
	grant, found := g[id]
	if !found || grant.ID != id || grant.Digest.isZero() {
		return Grant{}, false
	}
	return grant, true
}

type Grant struct {
	ID       ID
	Tenant   string
	Digest   Digest
	IssuedAt time.Time
}

func NewGrant(id ID, tenant string, digest Digest, issuedAt time.Time) (Grant, error) {
	if _, err := ParseID(string(id)); err != nil {
		return Grant{}, fmt.Errorf("grant id %q: %w", id, ErrInvalidGrant)
	}
	if tenant == "" {
		return Grant{}, fmt.Errorf("grant %s without a tenant: %w", id, ErrInvalidGrant)
	}
	if digest.isZero() {
		return Grant{}, fmt.Errorf("grant %s without a digest: %w", id, ErrInvalidGrant)
	}
	return Grant{ID: id, Tenant: tenant, Digest: digest, IssuedAt: issuedAt}, nil
}
