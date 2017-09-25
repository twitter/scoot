NAME := scoot
DESC := distributed build tools
GOVERSION := $(shell go version)
BUILDTIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
BUILDDATE := $(shell date -u +"%B %d, %Y")
PROJECT_URL := "https://github.com/twitter/scoot"

SHELL := /bin/bash -o pipefail

# Libaries don't configure the logger by default - define this so they can init the logger during testing.
SCOOT_LOGLEVEL ?= info

# Output can be overly long and exceed TravisCI 4MB limit, so filter out some of the noisier logs.
# Hacky redirect interactive console to 'tee /dev/null' so logrus on travis will produce full timestamps.
TRAVIS_FILTER ?= 2>&1 | tee /dev/null | egrep -v 'line="(runners|scheduler/task_|gitdb)'

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

coverage:
	sh testCoverage.sh $(TRAVIS_FILTER)

test-unit-property:
	# Runs only unit tests and property tests
	go test -race -timeout 120s -tags=property_test $$(go list ./... | grep -v /vendor/ | grep -v /cmd/) $(TRAVIS_FILTER)

test-unit:
	# Runs only unit tests
	# Only invoked manually so we don't need to modify output.
	go test -race -timeout 120s $$(go list ./... | grep -v /vendor/ | grep -v /cmd/)

test-integration:
	# Runs all tests including integration and property tests
	# We don't currently have any integration tests, but we're leaving this so we can add more.
	# Only invoked manually so we don't need to modify output.
	go test -race -timeout 120s -tags="integration property_test" $$(go list ./... | grep -v /vendor/ | grep -v /cmd/)

testlocal: generate test

testonly: test-unit-property

test: test-unit-property coverage

swarmtest:
	# Setup a local schedule against local workers (--strategy local.local)
	# Then run (with go run) scootapi run_smoke_test with 10 jobs, wait 1m
	# We build the binaries becuase 'go run' won't consistently pass signals to our program.
	go install ./binaries/...
	setup-cloud-scoot --strategy local.local run scootapi run_smoke_test --num_jobs 10 --timeout 1m $(TRAVIS_FILTER)

recoverytest:
	# Some overlap with swarmtest but focuses on sagalog recovery vs worker/checkout correctness.
	# We build the binaries becuase 'go run' won't consistently pass signals to our program.
	# Ignore output here to reduce travis log size. Swarmtest is more important and that still logs.
	go install ./binaries/...
	recoverytest &>/dev/null

clean-mockgen:
	rm */*_mock.go

clean-data:
	rm -rf ./.scootdata/*

clean-go:
	go clean ./...

clean: clean-data clean-mockgen clean-go

fullbuild: dependencies generate test

travis: dependencies recoverytest swarmtest test clean-data

thrift-worker:
	# Create generated code in github.com/twitter/scoot/workerapi/gen-go/... from worker.thrift
	cd workerapi && thrift --gen go:package_prefix=github.com/twitter/scoot/workerapi/gen-go/,package=worker,thrift_import=github.com/apache/thrift/lib/go/thrift worker.thrift && cd ..

thrift-sched:
	# Create generated code in github.com/twitter/scoot/sched/gen-go/... from job_def.thrift
	cd sched && thrift --gen go:package_prefix=github.com/twitter/scoot/sched/gen-go/,package=schedthrift,thrift_import=github.com/apache/thrift/lib/go/thrift job_def.thrift && cd ..

thrift-scoot:
	# Create generated code in github.com/twitter/scoot/scootapi/gen-go/... from scoot.thrift
	cd scootapi && thrift --gen go:package_prefix=github.com/twitter/scoot/scootapi/gen-go/,package=scoot,thrift_import=github.com/apache/thrift/lib/go/thrift scoot.thrift && cd ..

thrift: thrift-worker thrift-sched thrift-scoot
