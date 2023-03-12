
NAME := scoot
DESC := distributed build tools
GOVERSION := $(shell go version)
BUILDTIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
BUILDDATE := $(shell date -u +"%B %d, %Y")
PROJECT_URL := "https://github.com/wisechengyi/scoot"
FIRSTGOPATH := $(shell echo $${GOPATH%%:*})
THRIFTVERSION := $(shell thrift -version | cut -d ' ' -f 3 | tr -d '\n')
GO111MODULE := on
export GO111MODULE

SHELL := /bin/bash -o pipefail

# Libaries don't configure the logger by default - define this so they can init the logger during testing.
SCOOT_LOGLEVEL ?= info

build:
	go build ./...

fmt:
	gofmt -l -w -s .

vet:
	go vet ./...

install:
	go install ./...

# Cleans go.mod and go.sum of unused dependencies
tidy:
	go mod tidy

# Gets the version of REPO specified by VER
# Usage: make get REPO=cloud.google.com/go VER=v0.45.1
get:
	go get $(REPO)@$(VER)

# Gets the latest version of REPO, or if left blank, updates all modules,
# Usage: make get REPO=cloud.google.com/go
get_latest:
	go get -u $(REPO)

############## dependencies

# tool dependencies for developer workflows only (regenerating test mocks or thrift code)
dev-dependencies:
	# Install mockgen binary (it's only referenced for code gen, not imported directly.)
	# Both the binary and a mock checkout will be placed in $GOPATH
	go get github.com/golang/mock/mockgen@58cd061d09382b6011f84c1291ebe50ef2e25bab

	# check thrift, protoc versions
ifneq ($(THRIFTVERSION),0.9.3)
	echo "Wanted thrift version 0.9.3, got $(THRIFTVERSION)"
	exit 1
endif

############## tests and coverage

coverage: clean-procs
	bash scripts/test_coverage.sh

test: clean-procs
	go test -count=1 -race -timeout 120s $$(go list ./...)

############## standalone binary & integration tests

smoketest: clean-procs
	# Setup a local schedule against local workers (--strategy local.local)
	# Then run (with go run) scootcl smoketest with 10 jobs, wait 1m
	# We build the binaries because 'go run' won't consistently pass signals to our program.
	$(FIRSTGOPATH)/bin/setup-cloud-scoot --strategy local.local run scootcl smoketest --num_jobs 10 --timeout 2m

recoverytest: clean-procs
	# Some overlap with smoketest but focuses on sagalog recovery vs worker/checkout correctness.
	# We build the binaries because 'go run' won't consistently pass signals to our program.
	# Ignore output here to reduce ci log size. Smoketest is more important and that still logs.
	$(FIRSTGOPATH)/bin/recoverytest &>/dev/null

integrationtest: clean-procs
	# Integration test with some overlap with other standalone tests, but utilizes client binaries
	$(FIRSTGOPATH)/bin/scoot-integration &>/dev/null

############## cleanup

clean-mockgen:
	find . -name \*_mock.go -type f -delete

clean-data:
	rm -rf ./.scootdata/*

clean-go:
	go clean ./...

clean-procs:
	killall -SIGKILL scheduler workerserver apiserver || true

clean: clean-data clean-go clean-procs

############## code gen for mocks, bindata configs, thrift, and protoc

generate:
	go generate ./...

thrift-worker-go:
	# Create generated code in github.com/wisechengyi/scoot/worker/api/gen-go/... from worker/api/thrift/worker.thrift
	cd worker/domain && rm -rf gen-go && thrift --gen go:thrift_import=github.com/apache/thrift/lib/go/thrift ../api/thrift/worker.thrift && cd ../..
	rm -rf worker/domain/gen-go/worker/worker-remote/

thrift-sched-go:
	# Create generated code in github.com/wisechengyi/scoot/sched/gen-go/... from sched.thrift
	cd scheduler/domain && rm -rf gen-go && thrift --gen go:thrift_import=github.com/apache/thrift/lib/go/thrift sched.thrift && cd ../..
	rm -rf scheduler/domain/gen-go/sched/sched-remote/

thrift-scoot-go:
	# Create generated code in github.com/wisechengyi/scoot/scheduler/api/thrift/gen-go/... from scoot.thrift
	cd scheduler/api/thrift && rm -rf gen-go && thrift --gen go:thrift_import=github.com/apache/thrift/lib/go/thrift scoot.thrift && cd ../../..
	rm -rf scheduler/api/thrift/gen-go/scoot/cloud_scoot-remote/

thrift-go: thrift-sched-go thrift-scoot-go thrift-worker-go

thrift: thrift-go

############## top-level dev-fullbuild, ci targets

dev-fullbuild: dev-dependencies generate test coverage

bm: clean-procs
	go test -timeout 120s -bench=. $$(go list ./...) -run=^Bench

ci: clean-data install recoverytest smoketest integrationtest test bm coverage clean-data
