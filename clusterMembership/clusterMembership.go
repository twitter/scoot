package clusterMembership


type Node interface {
	GetId() string
	SendMessage(msg string) error
}

type Cluster interface {
	/*
	 * Returns a Slice of Node Ids in the Cluster
	 */
	GetMembers() []string

	/*
	 * Sends a Message to the specified Node in the Cluster,
	 * Returns an Error if the Node is not in the cluster or sending
	 * the message fails
	 */
	SendMessage(msg string, nodeId string) (error)
}
