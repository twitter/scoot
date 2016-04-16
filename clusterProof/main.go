package clusterProof

import "fmt"
import ci "scoot/clusterImplementations"

func main() {

	/*
	 * Create a Static Cluster and Send a Request to each node
	 */
	var cluster = ci.StaticLocalNodeClusterFactory(10)
	fmt.Println("clusterMembers:", cluster.GetMembers())

	for _, nodeId := range cluster.GetMembers() {
		cluster.SendMessage("Static Hello!", nodeId)
	}

	/*
	 * Create a Dynamic Cluster and Send a Request to each node
	 */
	var dynamicCluster = ci.DynamicLocalNodeClusterFactory(10)
	fmt.Println("clusterMembers:", dynamicCluster.GetMembers())

	for _, nodeId := range dynamicCluster.GetMembers() {
		dynamicCluster.SendMessage("Dynamic Hello!", nodeId)
	}
}
