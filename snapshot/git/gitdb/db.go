package gitdb

import (
	"fmt"
	"sync"

	"github.com/scootdev/scoot/os/temp"
	snap "github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/git/repo"
)

// snapshotKind describes the kind of a Snapshot: is it an FSSnapshot or a GitCommitSnapshot
// kind instead of type because type is a keyword
type snapshotKind string

const (
	kindFSSnapshot        snapshotKind = "fs"
	kindGitCommitSnapshot snapshotKind = "gc"
)

var kinds = map[snapshotKind]bool{
	kindFSSnapshot:        true,
	kindGitCommitSnapshot: true,
}

type AutoUploadDest int

const (
	AutoUploadNone AutoUploadDest = iota
	AutoUploadTags
)

// MakeDB makes a gitdb.DB that uses dataRepo for data and tmp for temporary directories
func MakeDB(dataRepo *repo.Repository, tmp *temp.TempDir, stream *StreamConfig,
	tags *TagsConfig, autoUploadDest AutoUploadDest) *DB {
	result := &DB{
		reqCh:     make(chan req),
		dataRepo:  dataRepo,
		tmp:       tmp,
		checkouts: make(map[string]bool),
		local:     &localBackend{},
		stream:    &streamBackend{cfg: stream},
		tags:      &tagsBackend{cfg: tags},
	}

	switch autoUploadDest {
	case AutoUploadNone:
	case AutoUploadTags:
		result.remote = result.tags
	default:
		panic(fmt.Errorf("unknown GitDB AutoUpload destination: %v", autoUploadDest))
	}

	go result.loop()
	return result
}

// TODO(dbentley): we may want more setup for our repo (e.g., disabling auto-gc)

// DB stores its data in a Git Repo
type DB struct {
	// DB uses a goroutine to serve requests, with requests of type req
	reqCh chan req

	// must hold workTreeLock before sending a checkoutReq to reqCh
	workTreeLock sync.Mutex

	// All data below here should be accessed only by the loop() goroutine
	dataRepo  *repo.Repository
	tmp       *temp.TempDir
	checkouts map[string]bool // checkouts stores bare checkouts, but not the git worktree
	local     *localBackend
	stream    *streamBackend
	tags      *tagsBackend
	remote    uploader // This is one of our backends that we use to upload automatically
	// TODO(dbentley): implement uploader
}

// req is a request interface
type req interface {
	req()
}

// Close stops the DB
func (db *DB) Close() {
	close(db.reqCh)
}

// loop loops serving requests serially
func (db *DB) loop() {
	for db.reqCh != nil {
		req, ok := <-db.reqCh
		if !ok {
			db.reqCh = nil
			continue
		}
		switch req := req.(type) {
		case ingestReq:
			s, err := db.ingestDir(req.dir)
			if err == nil && db.remote != nil {
				s, err = db.remote.upload(s, db)
			}
			if err != nil {
				req.resultCh <- idAndError{err: err}
			} else {
				req.resultCh <- idAndError{id: s.ID()}
			}
		case ingestGitCommitReq:
			s, err := db.ingestGitCommit(req.ingestRepo, req.commitish)
			if err == nil && db.remote != nil {
				s, err = db.remote.upload(s, db)
			}

			if err != nil {
				req.resultCh <- idAndError{err: err}
			} else {
				req.resultCh <- idAndError{id: s.ID()}
			}
		case checkoutReq:
			path, err := db.checkout(req.id)
			req.resultCh <- stringAndError{str: path, err: err}
		case releaseCheckoutReq:
			req.resultCh <- db.releaseCheckout(req.path)
		default:
			panic(fmt.Errorf("unknown reqtype: %T %v", req, req))
		}
	}
}

type ingestReq struct {
	dir      string
	resultCh chan idAndError
}

func (r ingestReq) req() {}

type idAndError struct {
	id  snap.ID
	err error
}

// IngestDir ingests a directory directly.
func (db *DB) IngestDir(dir string) (snap.ID, error) {
	resultCh := make(chan idAndError)
	db.reqCh <- ingestReq{dir: dir, resultCh: resultCh}
	result := <-resultCh
	return result.id, result.err
}

type ingestGitCommitReq struct {
	ingestRepo *repo.Repository
	commitish  string
	resultCh   chan idAndError
}

func (r ingestGitCommitReq) req() {}

// IngestGitCommit ingests the commit identified by commitish from ingestRepo
func (db *DB) IngestGitCommit(ingestRepo *repo.Repository, commitish string) (snap.ID, error) {
	resultCh := make(chan idAndError)
	db.reqCh <- ingestGitCommitReq{ingestRepo: ingestRepo, commitish: commitish, resultCh: resultCh}
	result := <-resultCh
	return result.id, result.err
}

type checkoutReq struct {
	id       snap.ID
	resultCh chan stringAndError
}

func (r checkoutReq) req() {}

type stringAndError struct {
	str string
	err error
}

// Checkout puts the snapshot identified by id in the local filesystem, returning
// the path where it lives or an error.
func (db *DB) Checkout(id snap.ID) (path string, err error) {
	db.workTreeLock.Lock()
	resultCh := make(chan stringAndError)
	db.reqCh <- checkoutReq{id: id, resultCh: resultCh}
	result := <-resultCh
	return result.str, result.err
}

type releaseCheckoutReq struct {
	path     string
	resultCh chan error
}

func (r releaseCheckoutReq) req() {}

// ReleaseCheckout releases a path from a previous Checkout. This allows Scoot to reuse
// the path. Scoot will not touch path after Checkout until ReleaseCheckout.
func (db *DB) ReleaseCheckout(path string) error {
	resultCh := make(chan error)
	db.reqCh <- releaseCheckoutReq{path: path, resultCh: resultCh}
	return <-resultCh
}

type downloadReq struct {
	id       snap.ID
	resultCh chan idAndError
}

func (r downloadReq) req() {}
