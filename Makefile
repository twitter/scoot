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
	# Checkout our vendored dependencies.
	# Note: The submodule dependencies must be initialized prior to running scoot binaries.
	#       When used as a library, the vendor folder will be empty by default (if 'go get'd). 
	git submodule update --init --recursive

	# Install mockgen binary (it's only referenced for code gen, not imported directly.)
	# Both the binary and a mock checkout will be placed in $GOPATH (duplicating the vendor checkout.)
	go get github.com/golang/mock/mockgen

update-dependencies:
	# TODO: some tools like pants (and glide?) require *all* transitive deps.
	#vendetta -u -p # Requires calling 'go get github.com/dpw/vendetta', only does direct deps.
	./deps.sh
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
