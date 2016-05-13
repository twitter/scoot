package cluster_membership

type Cluster interface {
	/*
	 * Returns a Slice of Node Ids in the Cluster
	 */
	Members() []Node
}
