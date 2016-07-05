package cluster_membership

//go:generate mockgen -source=cluster.go -package=cluster_membership -destination=cluster_mock.go

type Cluster interface {
	/*
	 * Returns a Slice of Node Ids in the Cluster
	 */
	Members() []Node
}
