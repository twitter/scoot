package min

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"runtime"
	"time"

	fs "github.com/twitter/scoot/fs/min/interface"
	"github.com/twitter/scoot/fs/min/state"
	"github.com/twitter/scoot/fuse"
)

func Serve(conn *fuse.Conn, rootFs fs.FS, threadUnsafe bool) (done chan error) {
	done = make(chan error, 1)

	root, err := rootFs.Root()
	if err != nil {
		conn.Close()
		return
	}
	serv := &servlet{state.MakeController(fuse.RootID, root, threadUnsafe)}

	numCPU := runtime.NumCPU() - 2 // Don't hog all the cores.
	if threadUnsafe || numCPU < 1 {
		numCPU = 1
	}
	for ii := 0; ii < numCPU; ii++ {
		go serve(conn, serv, done)
	}
	return
}

func serve(conn *fuse.Conn, serv *servlet, done chan error) {
	log.Infof("Serving ScootFS")
	var scope *fuse.RequestScope
	defer func() {
		if rec := recover(); rec != nil {
			const size = 1 << 16
			buf := make([]byte, size)
			n := runtime.Stack(buf, false)
			buf = buf[:n]
			if scope != nil {
				log.Infof("Panic recovered in handler: %v %v %v", rec, string(buf), scope.Req)
				scope.Resp.RespondError(fmt.Errorf("%v", rec), scope)
				scope.Release()
			} else {
				log.Infof("Panic recovered in handler: %v %v", rec, string(buf))
			}
			conn.Close()
			panic(rec)
		}
	}()
	defer func() {
		log.Infof("Signaling done")
		done <- nil
	}()

	alloc := fuse.MakeAlloc()
	var err error
	for {
		scope, err = conn.Read(alloc, serv)
		if err != nil {
			log.Infof("Error reading request; quitting: %v", err)
			done <- err
			conn.Close()
			return
		}
		scope.Release()
	}
}

var Trace bool

type servlet struct {
	controller *state.Controller
}

func (s *servlet) HandleStatfs(req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	return nil
}

func (s *servlet) HandleGetattr(req *fuse.GetattrRequest, resp *fuse.GetattrResponse) error {
	inode, err := s.controller.GetInode(req.Node())
	if err != nil {
		return fuse.ESTALE
	}
	attr, err := inode.GetNode().Attr()
	if err != nil {
		return err
	}
	attr.Inode = uint64(req.Node())
	resp.Attr(attr)
	return nil
}

func (s *servlet) HandleLookup(req *fuse.LookupRequest, resp *fuse.LookupResponse) error {
	nodeID := req.NodeID()
	inode, err := s.controller.GetInode(nodeID)
	if err != nil {
		return err
	}
	name := req.Name()
	newNode, err := inode.GetNode().Lookup(name)
	if err != nil {
		return err
	}
	newInodeID, err := s.controller.PutInode(nodeID, name, newNode)
	if err != nil {
		return err
	}
	attr, err := newNode.Attr()
	if err != nil {
		return err
	}
	attr.Inode = uint64(newInodeID)

	resp.NodeID(newInodeID)
	//NOTE(jschiller) OSXFuse ignores EntryValid. See github.com/osxfuse/osxfuse/issues/199
	resp.EntryValid(1 * time.Hour)
	// TODO(dbentley): we should probably sett AttrValid
	resp.Attr(attr)
	return nil
}

func (s *servlet) HandleReadlink(req *fuse.ReadlinkRequest, resp *fuse.ReadlinkResponse) error {
	inode, err := s.controller.GetInode(req.NodeID())
	if err != nil {
		return err
	}
	r, err := inode.GetNode().Readlink()
	if err != nil {
		return err
	}
	resp.Data(r)
	return nil
}

func (s *servlet) HandleOpendir(req *fuse.OpendirRequest, resp *fuse.OpendirResponse) error {
	inode, err := s.controller.GetInode(req.NodeID())
	if err != nil {
		return err
	}
	// TODO(dbentley): make an Opendir
	handle, err := inode.GetNode().Open()
	if err != nil {
		return err
	}

	newHandleId, err := s.controller.PutHandle(handle)
	if err != nil {
		return err
	}

	resp.Handle(newHandleId)
	resp.Flags(fuse.OpenKeepCache)
	return nil
}

func (s *servlet) HandleReaddir(req *fuse.ReaddirRequest, resp *fuse.ReaddirResponse) error {
	ihandle := s.controller.GetHandle(req.HandleID())
	if ihandle == nil {
		return fuse.ESTALE
	}

	cachedDirents, once := ihandle.GetCachedDirents()
	if !once {
		// The first caller to GetCachedDirents() will see once=true and will proceed to initialize the dirents.
		// First, initialize dirents with an empty slice in case we return early.
		ihandle.SetCachedDirents(make([]byte, 0))
		dirents, err := ihandle.GetHandle().ReadDirAll()
		if err != nil {
			return err
		}

		names := make([]string, len(dirents))
		for idx, de := range dirents {
			names[idx] = de.Name
		}

		var childrenInodes []fuse.NodeID
		childrenInodes, err = s.controller.ReserveChildren(req.NodeID(), names)
		if err != nil {
			log.Info(err)
			return err
		}

		l := 0
		for idx, _ := range dirents {
			l += dirents[idx].Size()
			dirents[idx].Inode = uint64(childrenInodes[idx])
		}

		all := make([]byte, 0, l)
		for _, de := range dirents {
			all = fuse.AppendDirent(all, de)
		}
		ihandle.SetCachedDirents(all)
		cachedDirents = all
	} else {
		// Another caller has already been flagged to initialize dirents, loop until we see the result.
		for cachedDirents == nil {
			cachedDirents, once = ihandle.GetCachedDirents()
		}
	}
	w := window(cachedDirents, req.Offset(), req.Size())
	resp.Data(w)
	return nil
}

func (s *servlet) HandleReleasedir(req *fuse.ReleasedirRequest, resp *fuse.ReleasedirResponse) error {
	handleID := req.HandleID()
	handle := s.controller.GetHandle(handleID)
	if handle == nil {
		return fuse.ESTALE
	}
	handle.GetHandle().Release()
	return s.controller.FreeHandle(handleID)
}

func (s *servlet) HandleOpen(req *fuse.OpenRequest, resp *fuse.OpenResponse) error {
	inode, err := s.controller.GetInode(req.NodeID())
	if err != nil {
		return err
	}
	// TODO(dbentley): make an Open
	handle, err := inode.GetNode().Open()
	if err != nil {
		return err
	}

	newHandleId, err := s.controller.PutHandle(handle)
	if err != nil {
		return err
	}

	resp.Handle(newHandleId)
	resp.Flags(fuse.OpenKeepCache)
	return nil
}

func (s *servlet) HandleRead(req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	ihandle := s.controller.GetHandle(req.HandleID())
	if ihandle == nil {
		return fuse.ESTALE
	}

	n, err := ihandle.GetHandle().ReadAt(resp.Data(), req.Offset())
	resp.Size(n)
	return err
}

func (s *servlet) HandleRelease(req *fuse.ReleaseRequest, resp *fuse.ReleaseResponse) error {
	handleID := req.HandleID()
	handle := s.controller.GetHandle(handleID)
	if handle == nil {
		return fuse.ESTALE
	}
	handle.GetHandle().Release()
	return s.controller.FreeHandle(handleID)
}

func window(data []byte, offset uint64, size uint32) []byte {
	if offset >= uint64(len(data)) {
		return nil
	}
	data = data[offset:]
	if uint32(len(data)) > size {
		data = data[:size]
	}
	return data
}
