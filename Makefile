NAME := scoot
DESC := distributed build tools
GOVERSION := $(shell go version)
BUILDTIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
BUILDDATE := $(shell date -u +"%B %d, %Y")
PROJECT_URL := "https://github.com/scootdev/scoot"

default: 
	go build ./...

dependencies: 
	go get -t github.com/scootdev/scoot/...

	# mockgen is only referenced for code gen, not imported directly
	go get github.com/golang/mock/mockgen 

test: 
	go generate ./...
	go test -v -race $$(go list ./... | grep -v /vendor/ | grep -v /cmd/)
	sh testCoverage.sh

clean-mockgen:
	rm */*_mock.go

clean: clean-mockgen
	go clean ./...
