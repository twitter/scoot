package gitdb

import (
	"fmt"
	"sync"
	"time"

	"github.com/wisechengyi/scoot/common/errors"
	"github.com/wisechengyi/scoot/common/stats"
	snap "github.com/wisechengyi/scoot/snapshot"
	"github.com/wisechengyi/scoot/snapshot/git/repo"
	"github.com/wisechengyi/scoot/snapshot/store"

	log "github.com/sirupsen/logrus"
)

// TODO The interfaces and functionality here should be refactored to only use git when necessary.
// Many operations relying on git are more inefficient than they need to be:
// we could be using memory buffers/streams instead of using git cmds on disk to transform data.

// MakeDBFromRepo makes a gitdb.DB that uses dataRepo for data and tmp for temporary directories
func MakeDBFromRepo(
	dataRepo *repo.Repository,
	updater RepoUpdater,
	tmp string,
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
	tmp string,
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
	tmp string,
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
	tmp        string          // path of a preexisting persistent directory used for temporary data
	checkouts  map[string]bool // checkouts stores bare checkouts, but not the git worktree
	local      *localBackend
	stream     *streamBackend
	tags       *tagsBackend
	bundles    *bundlestoreBackend
	autoUpload uploader // This is one of our backends that we use to upload automatically

	stat stats.StatsReceiver
}

// req is a request interface
type req interface {
	req()
}

// stringAndError contains a string field, where information such as a snapshot ID, path, or file contents can be stored,
// in addition to a Error field which contains both a standard error and an exit code
type stringAndError struct {
	str string
	err error
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

// loop loops serving requests serially, blocking only if there are multiple checkout requests.
// only one checkout is allowed at a time in the underlying repo, everything else should be ok if concurrent
func (db *DB) loop(initer RepoIniter) {
	if db.init(initer); db.err != nil {
		// we couldn't create our repo, so all operations will fail before
		// sending to reqCh, so we can stop serving
		return
	}
	// Handle checkout logic separately and block until each request completes
	checkoutCh := make(chan interface{})
	go func() {
		for req := range checkoutCh {
			switch req := req.(type) {
			case checkoutReq:
				path, err := db.checkout(req.id)
				req.resultCh <- stringAndError{str: path, err: err}
			case releaseCheckoutReq:
				req.resultCh <- db.releaseCheckout(req.path)
			}
		}
	}()

	// Handle all request types
	for db.reqCh != nil {
		req, ok := <-db.reqCh
		if !ok {
			db.reqCh = nil
			continue
		}
		switch req := req.(type) {
		case ingestReq:
			log.Debugf("processing ingestReq")
			go func() {
				s, err := db.ingestDir(req.dir)
				if err == nil && db.autoUpload != nil {
					s, err = db.autoUpload.upload(s, db)
				}
				if err != nil {
					req.resultCh <- idAndError{err: err}
				} else {
					req.resultCh <- idAndError{id: s.ID()}
				}
			}()
		case ingestGitCommitReq:
			log.Debugf("processing ingestGitCommitReq")
			go func() {
				s, err := db.ingestGitCommit(req.ingestRepo, req.commitish)
				if err == nil && db.autoUpload != nil {
					s, err = db.autoUpload.upload(s, db)
				}
				if err != nil {
					req.resultCh <- idAndError{err: err}
				} else {
					req.resultCh <- idAndError{id: s.ID()}
				}
			}()
		case ingestGitWorkingDirReq:
			log.Debugf("processing ingestGitWorkingDirReq")
			go func() {
				s, err := db.ingestGitWorkingDir(req.ingestRepo)
				if err == nil && db.autoUpload != nil {
					s, err = db.autoUpload.upload(s, db)
				}
				if err != nil {
					req.resultCh <- idAndError{err: err}
				} else {
					req.resultCh <- idAndError{id: s.ID()}
				}
			}()
		case uploadFileReq:
			log.Debugf("processing uploadFileReq")
			go func() {
				s, err := db.bundles.uploadFile(req.filePath, req.ttl)
				req.resultCh <- stringAndError{str: s, err: err}
			}()
		case readFileAllReq:
			log.Debugf("processing readFileAllReq")
			go func() {
				data, err := db.readFileAll(req.id, req.path)
				req.resultCh <- stringAndError{str: data, err: err}
			}()
		case checkoutReq, releaseCheckoutReq:
			log.Debugf("processing checkoutReq, releaseCheckoutReq")
			go func() {
				checkoutCh <- req
			}()
		case exportGitCommitReq:
			log.Debugf("processing exportGitCommitReq")
			go func() {
				sha, err := db.exportGitCommit(req.id, req.exportRepo)
				req.resultCh <- stringAndError{str: sha, err: err}
			}()
		case updateRepoReq:
			log.Debugf("processing updateRepoReq")
			go func() {
				req.resultCh <- db.updateRepo()
			}()
		default:
			panic(fmt.Errorf("unknown reqtype: %T %v", req, req))
		}
	}

	close(checkoutCh)
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

// ReadFileAll reads the contents of the file path in FSSnapshot ID, or errors
func (db *DB) ReadFileAll(id snap.ID, path string) ([]byte, error) {
	if <-db.initDoneCh; db.err != nil {
		return nil, errors.NewError(db.err, errors.DBInitFailureExitCode)
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
	log.Debugf("Checking out %s", id)
	if <-db.initDoneCh; db.err != nil {
		log.Error("Unable to init db")
		return "", errors.NewError(db.err, errors.DBInitFailureExitCode)
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
		return "", errors.NewError(db.err, errors.DBInitFailureExitCode)
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
	ttl      *store.TTLValue
	resultCh chan stringAndError
}

func (r uploadFileReq) req() {}

// Manual write of a file (like an existing bundle) to the underlying store
// Intended for HTTP-backed stores that implement bundlestore's TTL fields
// Base of the filePath will be used as bundle name
// Returns location of stored file
func (db *DB) UploadFile(filePath string, ttl *store.TTLValue) (string, error) {
	if <-db.initDoneCh; db.err != nil {
		return "", errors.NewError(db.err, errors.DBInitFailureExitCode)
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

// Unimplemented
func (db *DB) Cancel() error {
	return nil
}
