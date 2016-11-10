[![Build Status](https://travis-ci.org/scootdev/scoot.svg?branch=master)](https://travis-ci.org/scootdev/scoot)
[![codecov.io](https://codecov.io/github/Kitware/candela/coverage.svg?branch=master)](https://codecov.io/gh/scootdev/scoot?branch=master)

## Try It Out
Setup a scheduler and worker nodes on your laptop by running
```
go run ./binaries/setup-cloud-scoot/main.go --strategy local.local
```

Run a series of randomly generated tests against Scoot
```
go run ./binaries/scootapi/main.go run_smoke_test
```
