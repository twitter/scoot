package scootapi

const DefaultSched_Thrift string = "localhost:9090"
const DefaultSched_HTTP string = "localhost:9091"

const DefaultWorker_Thrift string = "localhost:9092"
const DefaultWorker_HTTP string = "localhost:9093"

const DefaultApiBundlestore_HTTP string = "localhost:9094"

// Port ranges to make setup of multiple workerServer/apiServer more repeatable.
const WorkerPorts = 10100
const ApiBundlestorePorts = 11100
