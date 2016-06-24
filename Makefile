NAME := scoot
DESC := distributed build tools
GOVERSION := $(shell go version)
BUILDTIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
BUILDDATE := $(shell date -u +"%B %d, %Y")
PROJECT_URL := "https://github.com/scootdev/scoot"

GO15VENDOREXPERIMENT := 1
export GO15VENDOREXPERIMENT

default:
	go build ./...

dependencies:
    # Checkout our vendored direct dependencies.
    # It's up to the library maintainers to vendor their dependencies (our transitive dependencies).
    # In the event that nested vendor dirs causes conflicts, we can flatten it / rewrite imports.
    # This approach is subject to change if golang ever formally blesses a different practice.
	git submodule update --init --recursive

    # Install mockgen binary (it's only referenced for code gen, not imported directly.)
    # Both the binary and a mock checkout will be placed in $GOPATH (duplicating the vendor checkout.)
	go get github.com/golang/mock/mockgen

update-dependencies:
	vendetta -u -p # Requires calling 'go get github.com/dpw/vendetta'
	go get -u github.com/golang/mock/mockgen

generate: 
	go generate $$(go list ./... | grep -v /vendor/)

format:
	go fmt $$(go list ./... | grep -v /vendor/)

vet:
	go vet $$(go list ./... | grep -v /vendor/)

test:
	go test -v -race $$(go list ./... | grep -v /vendor/ | grep -v /cmd/)
	sh testCoverage.sh

testlocal: generate test

clean-mockgen:
	rm */*_mock.go

clean: clean-mockgen
	go clean ./...

fullbuild: dependencies generate test
