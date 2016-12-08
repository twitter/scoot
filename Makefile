NAME := scoot
DESC := distributed build tools
GOVERSION := $(shell go version)
BUILDTIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
BUILDDATE := $(shell date -u +"%B %d, %Y")
PROJECT_URL := "https://github.com/scootdev/scoot"

GO15VENDOREXPERIMENT := 1
export GO15VENDOREXPERIMENT

default:
	go build $$(go list ./... | grep -v /vendor/)

dependencies:
	# Populates the vendor directory to reflect the latest run of check-dependencies.

	# Checkout our vendored dependencies.
	# Note: The submodule dependencies must be initialized prior to running scoot binaries.
	#       When used as a library, the vendor folder will be empty by default (if 'go get'd).
	git submodule update --init --recursive

	# Install mockgen binary (it's only referenced for code gen, not imported directly.)
	# Both the binary and a mock checkout will be placed in $GOPATH (duplicating the vendor checkout.)
	# We use 'go get' here because 'go install' will not build out of our vendored mock repo.
	go get github.com/golang/mock/mockgen

	# Install go-bindata tool which is used to generate binary version of config file
	# this is only used by go generate
	go get github.com/jteeuwen/go-bindata/...

check-dependencies:
	# Run this whenever a dependency is added.
	# We run our own script to get all transitive dependencies. See github.com/pantsbuild/pants/issues/3606.
	./deps.sh
	go get github.com/golang/mock/mockgen

generate:
	go generate $$(go list ./... | grep -v /vendor/)

format:
	go fmt $$(go list ./... | grep -v /vendor/)

vet:
	go vet $$(go list ./... | grep -v /vendor/)

test:
	# Runs only unit tests and property tests
	go install ./binaries/...
	go test -race -tags=property_test $$(go list ./... | grep -v /vendor/ | grep -v /cmd/)
	sh testCoverage.sh

test-unit:
	# Runs only unit tests
	go test -race $$(go list ./... | grep -v /vendor/ | grep -v /cmd/)

test-integration:
	# Runs all tests including integration and property tests
	# We don't currently have any integration tests, but we're leaving this so we can add more.
	go test -race -tags="integration property_test" $$(go list ./... | grep -v /vendor/ | grep -v /cmd/)

testlocal: generate test

swarmtest:
	# Setup a local schedule against local workers (--strategy local.local)
	# Then run (with go run) scootapi run_smoke_test with 10 jobs, wait 1m
	go run ./binaries/setup-cloud-scoot/main.go --strategy local.local run go run ./binaries/scootapi/main.go run_smoke_test 10 1m

clean-mockgen:
	rm */*_mock.go

clean: clean-mockgen
	go clean ./...

fullbuild: dependencies generate test

travis: fullbuild swarmtest
