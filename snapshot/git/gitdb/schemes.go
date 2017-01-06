package gitdb

import (
	"fmt"
	"strings"

	"github.com/scootdev/scoot/snapshot"
)

// GitDB uses several different Schemes to identify, upload and download Values.
// schemeValue abstracts these concerns.
type schemeValue interface {
	id() snapshot.ID

	// F suffix because name conflict between Function and member.
	kindF() valueKind
	shaF() string
	upload(db *DB) (snapshot.ID, error)
	download(db *DB) (snapshot.ID, error)
}

// parseID parses ID into a schemeValue
func parseID(id snapshot.ID) (schemeValue, error) {
	if id == "" {
		return nil, fmt.Errorf("empty snapshot ID")
	}
	parts := strings.Split(string(id), "-")
	if len(parts) < 3 {
		return nil, fmt.Errorf("could not determine scheme and kind in ID %s", id)
	}

	scheme, kind := parts[0], valueKind(parts[1])

	if !kinds[kind] {
		return nil, fmt.Errorf("invalid kind: %s", kind)
	}

	switch scheme {
	case localIDText:
		return parseLocalID(id, kind, parts[2:])
	case streamIDText:
		return parseStreamID(id, kind, parts[2:])
	default:
		return nil, fmt.Errorf("unrecognized snapshot scheme %s in ID %s", scheme, id)
	}
}

// validSha checks if sha is valid
func validSha(sha string) error {
	if len(sha) != 40 {
		return fmt.Errorf("sha not 40 characters: %s", sha)
	}
	// TODO(dbentley): check that it's hexadecimal?
	return nil
}

// download delegates to the value's implementation
func (db *DB) download(id snapshot.ID) (snapshot.ID, error) {
	v, err := parseID(id)
	if err != nil {
		return "", err
	}

	return v.download(db)
}
