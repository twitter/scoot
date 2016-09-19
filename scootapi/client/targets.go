package client

// the path to any pants tests that should pass go here,
// scootapi test_targets will test all of them.
// TODO(rcouto): move into a file and read from it
func getTargets() []string {
	return []string{"util/util-function/src/main/java:"}
}
