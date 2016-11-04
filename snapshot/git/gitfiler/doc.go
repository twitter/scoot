// gitfiler offers Scoot Snapshot Filer operations access to git
// It does this with a few related by separate structures:
//
// RepoIniter is an interface to initialize a Repository.
//
// refCloner gets new repos by running git clone --reference against a reference repo
//
// RepoPool controls concurrent access to repos
//
// Checkouter makes Checkouts by pulling a Repository from a RepoPool and performing a git checkout
//
// setup.go holds utility functions to create new Checkouters
package gitfiler
