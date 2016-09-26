[![Build Status](https://travis-ci.org/scootdev/scoot.svg?branch=caitie%2Fscheduler)](https://travis-ci.org/scootdev/scoot)
[![codecov.io](https://codecov.io/github/Kitware/candela/coverage.svg?branch=master)](https://codecov.io/gh/scootdev/scoot?branch=master)

## Testing
* make format
* make vet
* make test

### Swarm Test
Swarm Test will spin up a scheduler and cluster of worker nodes on your machine, send a bunch of randomly generated jobs to the server and then check the status of those jobs.  To run swarm test run:
```bash
go run ./binaries/swarmtest/main.go
```
