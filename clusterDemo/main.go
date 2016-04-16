package main

import "fmt"
import ci "scootdev/scoot/clusterimplementations"

func main() {

	staticCluster := ci.StaticLocalNodeClusterFactory(10)
	fmt.Println("clusterMembers:", staticCluster.GetMembers())

	for _, nodeId := range staticCluster.GetMembers() {
		staticCluster.SendMessage("Static Hello!", nodeId)
	}

	dynamicCluster := ci.DynamicLocalNodeClusterFactory(10)
	fmt.Println("clusterMembers:", dynamicCluster.GetMembers())

	for _, nodeId := range dynamicCluster.GetMembers() {
		dynamicCluster.SendMessage("Dynamic Hello!", nodeId)
	}
}
