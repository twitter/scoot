package gitdb

type value interface {
	value()
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
	default:
		return "", fmt.Errorf("unrecognized snapshot scheme %s in ID %s", scheme, id)
	}

	scheme, kind, sha := parts[0], valueKind(parts[1]), parts[2]
	if scheme != localIDText {
		return nil, fmt.Errorf("invalid scheme: %s", scheme)
	}

	if !kinds[kind] {
		return nil, fmt.Errorf("invalid kind: %s", kind)
	}

	if err := validSha(sha); err != nil {
		return nil, err
	}

	return &localValue{kind: kind, sha: sha}, nil
}
