NAME := scoot
DESC := distributed build tools
GOVERSION := $(shell go version)
BUILDTIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
BUILDDATE := $(shell date -u +"%B %d, %Y")
PROJECT_URL := "https://github.com/scootdev/scoot"

default:
	go build ./...

dependencies: 
	# get all dependencies needed to run scoot / check if there have
	# been updates since they were first installed
	go get -t -u github.com/scootdev/scoot/...

	# mockgen is only referenced for code gen, not imported directly
	go get -u github.com/golang/mock/mockgen

generate: 
	go generate ./...

test:
	go test -v -race $$(go list ./... | grep -v /vendor/ | grep -v /cmd/)
	sh testCoverage.sh

testlocal: generate test

clean-mockgen:
	rm */*_mock.go

clean: clean-mockgen
	go clean ./...

fullbuild: dependencies generate test