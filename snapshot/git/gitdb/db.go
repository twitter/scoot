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
	AutoUploadBundlestore
)

// MakeDBFromRepo makes a gitdb.DB that uses dataRepo for data and tmp for temporary directories
func MakeDBFromRepo(dataRepo *repo.Repository, tmp *temp.TempDir, stream *StreamConfig,
	tags *TagsConfig, bundles *BundlestoreConfig, autoUploadDest AutoUploadDest) *DB {
	return makeDB(dataRepo, nil, tmp, stream, tags, bundles, autoUploadDest)
}

// MakeDBNewRepo makes a gitDB that uses a new DB, populated by initer
func MakeDBNewRepo(initer RepoIniter, tmp *temp.TempDir, stream *StreamConfig,
	tags *TagsConfig, bundles *BundlestoreConfig, autoUploadDest AutoUploadDest) *DB {
	return makeDB(nil, initer, tmp, stream, tags, bundles, autoUploadDest)
}

func makeDB(dataRepo *repo.Repository, initer RepoIniter, tmp *temp.TempDir, stream *StreamConfig,
	tags *TagsConfig, bundles *BundlestoreConfig, autoUploadDest AutoUploadDest) *DB {
	if (dataRepo == nil) == (initer == nil) {
		panic(fmt.Errorf("exactly one of dataRepo and initer must be non-nil in call to makeDB: %v %v", dataRepo, initer))
	}
	result := &DB{
		reqCh:      make(chan req),
		initDoneCh: make(chan struct{}),
		dataRepo:   dataRepo,
		tmp:        tmp,
		checkouts:  make(map[string]bool),
		local:      &localBackend{},
		stream:     &streamBackend{cfg: stream},
		tags:       &tagsBackend{cfg: tags},
		bundles:    &bundlestoreBackend{cfg: bundles},
	}

	switch autoUploadDest {
	case AutoUploadNone:
	case AutoUploadTags:
		result.autoUpload = result.tags
	case AutoUploadBundlestore:
		result.autoUpload = result.bundles
	default:
		panic(fmt.Errorf("unknown GitDB AutoUpload destination: %v", autoUploadDest))
	}

	go result.loop(initer)
	return result
}

type RepoIniter interface {
	Init() (*repo.Repository, error)
}

// DB stores its data in a Git Repo
type DB struct {
	// DB uses a goroutine to serve requests, with requests of type req
	reqCh chan req

	// Our init can fail, and if it did, err will be non-nil, so before sending
	// to reqCh, read from initDoneCh (which will be closed after initialization is done)
	// and test if err is non-nil
	initDoneCh chan struct{}
	err        error

	// must hold workTreeLock before sending a checkoutReq to reqCh
	workTreeLock sync.Mutex

	// All data below here should be accessed only by the loop() goroutine
	dataRepo   *repo.Repository
	tmp        *temp.TempDir
	checkouts  map[string]bool // checkouts stores bare checkouts, but not the git worktree
	local      *localBackend
	stream     *streamBackend
	tags       *tagsBackend
	bundles    *bundlestoreBackend
	autoUpload uploader // This is one of our backends that we use to upload automatically
}

// req is a request interface
type req interface {
	req()
}

// Close stops the DB
func (db *DB) Close() {
	close(db.reqCh)
}

// initialize our repo (if necessary)
func (db *DB) init(initer RepoIniter) {
	defer close(db.initDoneCh)
	if initer != nil {
		db.dataRepo, db.err = initer.Init()
	}
}

// loop loops serving requests serially
func (db *DB) loop(initer RepoIniter) {
	if db.init(initer); db.err != nil {
		// we couldn't create our repo, so all operations will fail before
		// sending to reqCh, so we can stop serving
		return
	}

	for db.reqCh != nil {
		req, ok := <-db.reqCh
		if !ok {
			db.reqCh = nil
			continue
		}
		switch req := req.(type) {
		case ingestReq:
			s, err := db.ingestDir(req.dir)
			if err == nil && db.autoUpload != nil {
				s, err = db.autoUpload.upload(s, db)
			}
			if err != nil {
				req.resultCh <- idAndError{err: err}
			} else {
				req.resultCh <- idAndError{id: s.ID()}
			}
		case ingestGitCommitReq:
			s, err := db.ingestGitCommit(req.ingestRepo, req.commitish)
			if err == nil && db.autoUpload != nil {
				s, err = db.autoUpload.upload(s, db)
			}

			if err != nil {
				req.resultCh <- idAndError{err: err}
			} else {
				req.resultCh <- idAndError{id: s.ID()}
			}
		case readFileAllReq:
			data, err := db.readFileAll(req.id, req.path)
			req.resultCh <- stringAndError{str: data, err: err}
		case checkoutReq:
			path, err := db.checkout(req.id)
			req.resultCh <- stringAndError{str: path, err: err}
		case releaseCheckoutReq:
			req.resultCh <- db.releaseCheckout(req.path)
		case exportGitCommitReq:
			sha, err := db.exportGitCommit(req.id, req.exportRepo)
			req.resultCh <- stringAndError{str: sha, err: err}
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
	if <-db.initDoneCh; db.err != nil {
		return "", db.err
	}
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
	if <-db.initDoneCh; db.err != nil {
		return "", db.err
	}
	resultCh := make(chan idAndError)
	db.reqCh <- ingestGitCommitReq{ingestRepo: ingestRepo, commitish: commitish, resultCh: resultCh}
	result := <-resultCh
	return result.id, result.err
}

type readFileAllReq struct {
	id       snap.ID
	path     string
	resultCh chan stringAndError
}

func (r readFileAllReq) req() {}

type stringAndError struct {
	str string
	err error
}

// ReadFileAll reads the contents of the file path in FSSnapshot ID, or errors
func (db *DB) ReadFileAll(id snap.ID, path string) ([]byte, error) {
	if <-db.initDoneCh; db.err != nil {
		return nil, db.err
	}
	resultCh := make(chan stringAndError)
	db.reqCh <- readFileAllReq{id: id, path: path, resultCh: resultCh}
	result := <-resultCh
	return []byte(result.str), result.err
}

type checkoutReq struct {
	id       snap.ID
	resultCh chan stringAndError
}

func (r checkoutReq) req() {}

// Checkout puts the snapshot identified by id in the local filesystem, returning
// the path where it lives or an error.
func (db *DB) Checkout(id snap.ID) (path string, err error) {
	if <-db.initDoneCh; db.err != nil {
		return "", db.err
	}
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
	if <-db.initDoneCh; db.err != nil {
		return db.err
	}
	resultCh := make(chan error)
	db.reqCh <- releaseCheckoutReq{path: path, resultCh: resultCh}
	return <-resultCh
}

type exportGitCommitReq struct {
	id         snap.ID
	exportRepo *repo.Repository
	resultCh   chan stringAndError
}

func (r exportGitCommitReq) req() {}

func (db *DB) ExportGitCommit(id snap.ID, exportRepo *repo.Repository) (string, error) {
	if <-db.initDoneCh; db.err != nil {
		return "", db.err
	}
	resultCh := make(chan stringAndError)
	db.reqCh <- exportGitCommitReq{id: id, exportRepo: exportRepo, resultCh: resultCh}
	result := <-resultCh
	return result.str, result.err
}

type downloadReq struct {
	id       snap.ID
	resultCh chan idAndError
}

func (r downloadReq) req() {}

func (db *DB) IDForStreamCommitSHA(streamName string, sha string) snap.ID {
	s := &streamSnapshot{sha: sha, kind: kindGitCommitSnapshot, streamName: streamName}
	return s.ID()
}
