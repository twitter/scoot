NAME := scoot
DESC := distributed build tools
GOVERSION := $(shell go version)
BUILDTIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
BUILDDATE := $(shell date -u +"%B %d, %Y")
PROJECT_URL := "https://github.com/scootdev/scoot"

default: 
	go build ./...

test: mockgen
	go test -v -race $$(go list ./... | grep -v /vendor/ | grep -v /cmd/)
	sh testCoverage.sh

mockgen:
	mockgen -source=saga/sagalog.go -package=saga -destination=saga/sagalog_mock.go

clean-mockgen:
	rm saga/sagalog_mock.go

clean: clean-mockgen
	go clean ./...
