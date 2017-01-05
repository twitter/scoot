package gitdb

import (
	"fmt"

	"github.com/scootdev/scoot/snapshot"
)

func (db *DB) download(id snapshot.ID) (snapshot.ID, error) {
	v, err := parseID(id)
	if err != nil {
		return "", err
	}

	switch v := v.(type) {
	case *localValue:
		return id, nil
	case *streamValue:
		return db.downloadStreamValue(v)
	default:
		return "", fmt.Errorf("cannot download: unrecognized value type %T %v", v, v)
	}

	return "", fmt.Errorf("not yet implemented")
}
