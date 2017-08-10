package gitdb

import (
	"fmt"
	"sync"
	"time"

	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/os/temp"
	snap "github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/bundlestore"
	"github.com/twitter/scoot/snapshot/git/repo"
)

// SnapshotKind describes the kind of a Snapshot: is it an FSSnapshot or a GitCommitSnapshot
// kind instead of type because type is a keyword
type SnapshotKind string

const (
	KindFSSnapshot        SnapshotKind = "fs"
	KindGitCommitSnapshot SnapshotKind = "gc"
)

var kinds = map[SnapshotKind]bool{
	KindFSSnapshot:        true,
	KindGitCommitSnapshot: true,
}

type AutoUploadDest int

const (
	AutoUploadNone AutoUploadDest = iota
	AutoUploadTags
	AutoUploadBundlestore
)

// MakeDBFromRepo makes a gitdb.DB that uses dataRepo for data and tmp for temporary directories
func MakeDBFromRepo(
	dataRepo *repo.Repository,
	updater RepoUpdater,
	tmp *temp.TempDir,
	stream *StreamConfig,
	tags *TagsConfig,
	bundles *BundlestoreConfig,
	autoUploadDest AutoUploadDest,
	stat stats.StatsReceiver) *DB {
	return makeDB(dataRepo, nil, updater, tmp, stream, tags, bundles, autoUploadDest, stat)
}

// MakeDBNewRepo makes a gitDB that uses a new DB, populated by initer
func MakeDBNewRepo(
	initer RepoIniter,
	updater RepoUpdater,
	tmp *temp.TempDir,
	stream *StreamConfig,
	tags *TagsConfig,
	bundles *BundlestoreConfig,
	autoUploadDest AutoUploadDest,
	stat stats.StatsReceiver) *DB {
	return makeDB(nil, initer, updater, tmp, stream, tags, bundles, autoUploadDest, stat)
}

func makeDB(
	dataRepo *repo.Repository,
	initer RepoIniter,
	updater RepoUpdater,
	tmp *temp.TempDir,
	stream *StreamConfig,
	tags *TagsConfig,
	bundles *BundlestoreConfig,
	autoUploadDest AutoUploadDest,
	stat stats.StatsReceiver) *DB {
	if (dataRepo == nil) == (initer == nil) {
		panic(fmt.Errorf("exactly one of dataRepo and initer must be non-nil in call to makeDB: %v %v", dataRepo, initer))
	}
	result := &DB{
		initDoneCh: make(chan error),
		InitDoneCh: make(chan error, 1),
		reqCh:      make(chan req),
		dataRepo:   dataRepo,
		updater:    updater,
		tmp:        tmp,
		checkouts:  make(map[string]bool),
		local:      &localBackend{},
		stream:     &streamBackend{cfg: stream, stat: stat},
		tags:       &tagsBackend{cfg: tags},
		bundles:    &bundlestoreBackend{cfg: bundles},
		stat:       stat,
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

// Interface for defining initialization that results in a valid repo.Repository.
// Distinct from gitfiler.RepoIniter
type RepoIniter interface {
	Init() (*repo.Repository, error)
}

// Interface for defining update behavior for an underlying repo.Repository.
// Provides an update mechanism and an interval definition
type RepoUpdater interface {
	Update(r *repo.Repository) error
	UpdateInterval() time.Duration
}

// DB stores its data in a Git Repo
type DB struct {
	// DB uses a goroutine to serve requests, with requests of type req
	reqCh chan req

	// Our init can fail, and if it did, err will be non-nil, so before sending
	// to reqCh, read from initDoneCh (which will be closed after initialization is done)
	// and test if err is non-nil.
	// Using both an internal initDoneCh and a public InitDoneCh for external consumption.
	initDoneCh chan error
	InitDoneCh chan error
	err        error

	// must hold workTreeLock before sending a checkoutReq to reqCh
	workTreeLock sync.Mutex

	// All data below here should be accessed only by the loop() goroutine
	dataRepo   *repo.Repository
	updater    RepoUpdater
	tmp        *temp.TempDir
	checkouts  map[string]bool // checkouts stores bare checkouts, but not the git worktree
	local      *localBackend
	stream     *streamBackend
	tags       *tagsBackend
	bundles    *bundlestoreBackend
	autoUpload uploader // This is one of our backends that we use to upload automatically

	// TODO: reusing git checkout if its snap.ID matches the request - make this configurable at runtime...
	currentSnapID snap.ID

	stat stats.StatsReceiver
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
	defer close(db.InitDoneCh)
	if initer != nil {
		db.dataRepo, db.err = initer.Init()
		db.InitDoneCh <- db.err
	}
}

// Update our repo with underlying RepoUpdater if provided
func (db *DB) updateRepo() error {
	if db.updater == nil {
		return nil
	}
	return db.updater.Update(db.dataRepo)
}

// Returns RepoUpdater's interval if an updater exists, or a zero duration
func (db *DB) UpdateInterval() time.Duration {
	if db.updater == nil {
		return snap.NoDuration
	}
	return db.updater.UpdateInterval()
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
		case ingestGitWorkingDirReq:
			s, err := db.ingestGitWorkingDir(req.ingestRepo)
			if err == nil && db.autoUpload != nil {
				s, err = db.autoUpload.upload(s, db)
			}
			if err != nil {
				req.resultCh <- idAndError{err: err}
			} else {
				req.resultCh <- idAndError{id: s.ID()}
			}
		case uploadFileReq:
			s, err := db.bundles.uploadFile(req.filePath, req.ttl)
			req.resultCh <- stringAndError{str: s, err: err}
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
		case updateRepoReq:
			req.resultCh <- db.updateRepo()
		default:
			panic(fmt.Errorf("unknown reqtype: %T %v", req, req))
		}
	}
}

// Request entry points and request/result type defs

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

type ingestGitWorkingDirReq struct {
	ingestRepo *repo.Repository
	resultCh   chan idAndError
}

func (r ingestGitWorkingDirReq) req() {}

// IngestGitWorkingDir ingests HEAD + working dir modifications from the ingestRepo.
func (db *DB) IngestGitWorkingDir(ingestRepo *repo.Repository) (snap.ID, error) {
	if <-db.initDoneCh; db.err != nil {
		return "", db.err
	}
	resultCh := make(chan idAndError)
	db.reqCh <- ingestGitWorkingDirReq{ingestRepo: ingestRepo, resultCh: resultCh}
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

// ExportGitCommit applies a snapshot to a repository, returning the sha of
// the exported commit or an error.
func (db *DB) ExportGitCommit(id snap.ID, exportRepo *repo.Repository) (string, error) {
	if <-db.initDoneCh; db.err != nil {
		return "", db.err
	}
	resultCh := make(chan stringAndError)
	db.reqCh <- exportGitCommitReq{id: id, exportRepo: exportRepo, resultCh: resultCh}
	result := <-resultCh
	return result.str, result.err
}

type updateRepoReq struct {
	resultCh chan error
}

func (r updateRepoReq) req() {}

// Update is used to trigger an underlying RepoUpdater to Update() the Repository.
// If db.updater is nil, has no effect.
func (db *DB) Update() error {
	if <-db.initDoneCh; db.err != nil {
		return db.err
	}
	resultCh := make(chan error)
	db.reqCh <- updateRepoReq{resultCh: resultCh}
	result := <-resultCh
	return result
}

// Below functions are utils not part of DB interface

// IDForStreamCommitSHA gets a SnapshotID from a string name and commit sha
func (db *DB) IDForStreamCommitSHA(streamName string, sha string) snap.ID {
	s := &streamSnapshot{sha: sha, kind: KindGitCommitSnapshot, streamName: streamName}
	return s.ID()
}

type uploadFileReq struct {
	filePath string
	ttl      *bundlestore.TTLValue
	resultCh chan stringAndError
}

func (r uploadFileReq) req() {}

// Manual write of a file (like an existing bundle) to the underlying store
// Intended for HTTP-backed stores that implement bundlestore's TTL fields
// Base of the filePath will be used as bundle name
// Returns location of stored file
func (db *DB) UploadFile(filePath string, ttl *bundlestore.TTLValue) (string, error) {
	if <-db.initDoneCh; db.err != nil {
		return "", db.err
	}
	resultCh := make(chan stringAndError)
	db.reqCh <- uploadFileReq{filePath: filePath, ttl: ttl, resultCh: resultCh}
	result := <-resultCh
	return result.str, result.err
}

func (db *DB) StreamName() string {
	if db.stream != nil && db.stream.cfg != nil {
		return db.stream.cfg.Name
	}
	return ""
}
