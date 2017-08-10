package state

// The dispatch state of our filesystem. This maps inode numbers to our node objects and
// handle ids to our handle objects.

import (
	"sync"

	fs "github.com/twitter/scoot/fs/min/interface"
	"github.com/twitter/scoot/fuse"
)

type inode struct {
	node     fs.Node
	nodeID   fuse.NodeID
	children map[string]fuse.NodeID
}

// Wraps a handle and its cached dirents. Cache may see concurrent sets/gets hence the locking.
// Note: the process of creating cached dirents mutates Controller.inodes and the parent inode's children.
type handle struct {
	handle       fs.Handle
	dirents      []byte
	mutex        sync.Mutex
	once         bool
	threadUnsafe bool
}

// Performance-Driven
//   Controller.inodes was converted from map to array to reduce hashing.
// Problem
//   Profiling showed that hashing was taking 14% of total exec time. This is too high.
// Improvement
//   Total exec time for indoes operations has been reduced to 0%.
// Caveats
//   This currently grows without bound. We will eventually want to handle forget requests without compromising on perf.
// How To Test
//   Run go pprof for the default 30s then type 'web' to get a call graph.
//   Look for a Controller.getInode() entry and cost. If absent, it's taking a relatively negligible amount of time.
type Controller struct {
	inodes       []*inode
	handles      []*handle
	mutex        sync.Mutex
	threadUnsafe bool
}

func MakeController(rootID fuse.NodeID, root fs.Node, threadUnsafe bool) *Controller {
	c := &Controller{threadUnsafe: threadUnsafe}
	rootIdx := int(rootID)             // 0 is unused/not found, 1 is root
	c.inodes = make([]*inode, 2, 8196) //capacity is arbitrary and could be left blank.
	for ii := 0; ii <= rootIdx; ii++ {
		c.inodes[ii] = &inode{}
	}
	*c.inodes[rootIdx] = inode{root, rootID, make(map[string]fuse.NodeID)}
	return c
}

// All callers must obtain a lock before calling this func.
// Note: returns (reservedInode,false) if an inode is already reserved but uninitialized, in case someone cares.
func (c *Controller) toInode(nodeID fuse.NodeID) (*inode, bool) {
	if nodeID == 0 || int(nodeID) >= len(c.inodes) {
		return nil, false
	}
	ino := c.inodes[int(nodeID)]
	return ino, ino.node != nil
}

// Return the specified inode or fuse.ESTALE if it's missing or uninitialized.
func (c *Controller) GetInode(nodeID fuse.NodeID) (*inode, error) {
	if !c.threadUnsafe {
		c.mutex.Lock()
		defer c.mutex.Unlock()
	}
	ino, ok := c.toInode(nodeID)
	if !ok {
		return nil, fuse.ESTALE
	}
	return ino, nil
}

// Inodes must be reservable to serve as forward declarations for children that haven't been looked up yet.
func (c *Controller) reserveInode(parentInode *inode, name string) fuse.NodeID {
	if !c.threadUnsafe {
		c.mutex.Lock()
		defer c.mutex.Unlock()
	}
	newID := fuse.NodeID(len(c.inodes))
	c.inodes = append(c.inodes, &inode{})
	parentInode.children[name] = newID
	return newID
}

// Reserve each name in both Controller.inodes and parentInode.children.
// If a name has already been reserved then this uses the existing entries.
func (c *Controller) ReserveChildren(parentID fuse.NodeID, names []string) ([]fuse.NodeID, error) {
	if !c.threadUnsafe {
		c.mutex.Lock()
		defer c.mutex.Unlock()
	}
	parentInode, err := c.GetInode(parentID)
	if err != nil {
		return nil, err
	}

	children := make([]fuse.NodeID, len(names))
	for idx, name := range names {
		nodeID, ok := parentInode.children[name]
		if !ok {
			nodeID = c.reserveInode(parentInode, name)
		}
		children[idx] = nodeID
	}
	return children, nil
}

// Makes sure that both our list of inodes and the parent inode have a reference to the named child node.
// If an inode has already been reserved then initialize it, otherwise construct a new one and set it.
func (c *Controller) PutInode(parentID fuse.NodeID, name string, node fs.Node) (fuse.NodeID, error) {
	if !c.threadUnsafe {
		c.mutex.Lock()
		defer c.mutex.Unlock()
	}
	parentInode, ok := c.toInode(parentID)
	if !ok {
		return 0, fuse.ESTALE
	}

	newID, ok := parentInode.children[name]
	if !ok {
		newID = fuse.NodeID(len(c.inodes))
		parentInode.children[name] = newID
		newInode := &inode{node, newID, make(map[string]fuse.NodeID)}
		c.inodes = append(c.inodes, newInode)
	} else {
		// The inode may or may not be initialized already, reset its contents either way.
		ino, _ := c.toInode(newID)
		ino.node = node
		ino.nodeID = newID
		ino.children = make(map[string]fuse.NodeID)
	}
	return newID, nil
}

// Returns the specified handle if it exists, otherwise nil.
// Assumes that freeHandle() will not be called while the caller is using this handle.
func (c *Controller) GetHandle(handleID fuse.HandleID) *handle {
	if !c.threadUnsafe {
		c.mutex.Lock()
		defer c.mutex.Unlock()
	}
	if int(handleID) >= len(c.handles) {
		return nil
	}
	return c.handles[handleID]
}

// Sets the first free handle in c.handles. If c.handles is full, append a new handle.
func (c *Controller) PutHandle(hand fs.Handle) (fuse.HandleID, error) {
	if !c.threadUnsafe {
		c.mutex.Lock()
		defer c.mutex.Unlock()
	}
	for idx, h := range c.handles {
		if h == nil {
			c.handles[idx] = &handle{handle: hand, dirents: nil, threadUnsafe: c.threadUnsafe}
			return fuse.HandleID(idx), nil
		}
	}
	c.handles = append(c.handles, &handle{handle: hand, dirents: nil, threadUnsafe: c.threadUnsafe})
	return fuse.HandleID(len(c.handles) - 1), nil
}

// Frees the specified handle if it exists, otherwise returns ESTALE.
// Note that cached dirents are freed but the associated reserved inodes currently remain.
func (c *Controller) FreeHandle(handleID fuse.HandleID) error {
	if !c.threadUnsafe {
		c.mutex.Lock()
		defer c.mutex.Unlock()
	}
	if int(handleID) >= len(c.handles) {
		return fuse.ESTALE
	}
	c.handles[handleID] = nil
	return nil
}

// Returns nil if dirents haven't been cached yet.
// Flag 'once' is false for the first caller and true for all subsequent callers.
func (h *handle) GetCachedDirents() (dirents []byte, once bool) {
	if !h.threadUnsafe {
		h.mutex.Lock()
		defer h.mutex.Unlock()
	}
	once = h.once
	h.once = true
	return h.dirents, once
}

// It should be sufficient to set the cache one but multiple times is allowed.
func (h *handle) SetCachedDirents(dirents []byte) {
	if !h.threadUnsafe {
		h.mutex.Lock()
		defer h.mutex.Unlock()
	}
	h.dirents = dirents
}

// Simple getter for hidden handle field.
func (h *handle) GetHandle() fs.Handle {
	return h.handle
}

// Simple getter for hidden node field.
func (i *inode) GetNode() fs.Node {
	return i.node
}
