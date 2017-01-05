package gitdb

type value interface {
	ID() snapshot.ID
}

// parseID parses ID into a localValue
func parseID(id snapshot.ID) (value, error) {
	if id == "" {
		return nil, fmt.Errorf("empty snapshot ID")
	}
	parts := strings.SplitN(string(id), "-", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("could not determine scheme in ID %s", id)
	}

	scheme := parts[0]

	switch scheme {
	case localIDText:
		return parseLocalID(id)
	case streamIDText:
		return parseStreamID(id)
	default:
		return "", fmt.Errorf("unrecognized snapshot scheme %s in ID %s", scheme, id)
	}
}
