package gitdb

// A Stream is a sequence of SnapshotWithHistory's.
type streamConfig struct {
	name    string
	refSpec string
}

const streamIDText = "stream"
const streamIDFmt = "%s-%s-%s"

func parseStreamID(id snapshot.ID) (*streamValue, error) {
	parts := strings.Split(string(id), "-")
	if len(parts) != 3 {
		return nil, fmt.Errorf("cannot parse snapshot ID: expected 2 hyphens in local id %s", id)
	}
	scheme, streamName, sha := parts[0], valueKind(parts[1]), parts[2]
	if scheme != streamIDText {
		return nil, fmt.Errorf("scheme not stream: %s", id)
	}

	if err := validSha(sha); err != nil {
		return nil, err
	}

	return &streamValue{streamName: streamName, sha: sha}, nil
}

type streamValue struct {
	streamName string
	sha        string
}

func (v *streamValue) ID() {
	return snapshot.ID(fmt.Sprintf(streamIDFmt, streamIDText, v.streamName, v.sha))
}
