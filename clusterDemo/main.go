package main

import "fmt"
import ci "scootdev/scoot/clusterimplementations"

func main() {

	staticCluster := ci.StaticLocalNodeClusterFactory(10)
	fmt.Println("clusterMembers:", staticCluster.Members())

	for _, node := range staticCluster.Members() {
		node.SendMessage("Static Hello!")
	}

	dynamicCluster := ci.DynamicLocalNodeClusterFactory(10)
	fmt.Println("clusterMembers:", dynamicCluster.Members())

	for _, node := range dynamicCluster.Members() {
		node.SendMessage("Dynamic Hello!")
	}
}
