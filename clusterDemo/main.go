package main

import "fmt"
import ci "scootdev/scoot/clusterimplementations"

func main() {

	staticCluster := ci.StaticLocalNodeClusterFactory(10)
	fmt.Println("clusterMembers:", staticCluster.Members())

	for _, nodeId := range staticCluster.Members() {
		staticCluster.SendMessage("Static Hello!", nodeId)
	}

	dynamicCluster := ci.DynamicLocalNodeClusterFactory(10)
	fmt.Println("clusterMembers:", dynamicCluster.Members())

	for _, nodeId := range dynamicCluster.Members() {
		dynamicCluster.SendMessage("Dynamic Hello!", nodeId)
	}
}
