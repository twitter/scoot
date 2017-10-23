package scootapi

const DefaultSched_Thrift string = "localhost:9090"
const DefaultSched_HTTP string = "localhost:9091"
const DefaultSched_GRPC string = "localhost:9099"

const DefaultWorker_Thrift string = "localhost:9092"
const DefaultWorker_HTTP string = "localhost:9093"

const DefaultApiBundlestore_HTTP string = "localhost:9094"
const DefaultApiBundlestore_GRPC string = "localhost:9098"

// Port ranges to make setup of multiple workerServer/apiServer more repeatable.
const WorkerPorts = 10100
const ApiBundlestorePorts = 11100

const BundlestoreEnvVar = "BUNDLESTORE_STORE_DIR"
