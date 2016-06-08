NAME := scoot
DESC := distributed build tools
GOVERSION := $(shell go version)
BUILDTIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
BUILDDATE := $(shell date -u +"%B %d, %Y")
PROJECT_URL := "https://github.com/scootdev/scoot"

default: 
	go build ./...

dependencies: 
	go get github.com/golang/mock/gomock
	go get github.com/golang/mock/mockgen
	go get github.com/onsi/gomega
	go get github.com/onsi/ginkgo
	go get github.com/spf13/cobra
	go get golang.org/x/net/context
	go get golang.org/x/tools/cmd/cover
	go get google.golang.org/grpc

test: 
	go generate ./...
	go test -v -race $$(go list ./... | grep -v /vendor/ | grep -v /cmd/)
	sh testCoverage.sh

clean-mockgen:
	rm */*_mock.go

clean: clean-mockgen
	go clean ./...
