package main

import "fmt"
import ci "scoot/clusterImplementations"

func main() {

	var cluster = ci.StaticLocalNodeClusterFactory(10)
	fmt.Println("clusterMembers:", cluster.GetMembers())

	for _, nodeId := range cluster.GetMembers() {
		cluster.SendMessage("Static Hello!", nodeId)
	}

	var dynamicCluster = ci.DynamicLocalNodeClusterFactory(10)
	fmt.Println("clusterMembers:", dynamicCluster.GetMembers())

	for _, nodeId := range dynamicCluster.GetMembers() {
		dynamicCluster.SendMessage("Dynamic Hello!", nodeId)
	}
}
