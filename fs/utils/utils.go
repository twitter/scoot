package utils

import (
	"reflect"
	"sync"
	"unsafe"
)

const maxPathLen = 4096

// Periodically reallocated pool for callers that store their strings.
const poolSize int = 16384 //Number of elements before we must reallocate.
var pool [][maxPathLen]byte = make([][maxPathLen]byte, poolSize)
var headerPool []string = make([]string, poolSize)
var poolIdx int = -1

// Fixed pool for callers that discard their strings.
const fixedPoolSize int = 1024 //Should be overkill; max borrowed elements at once.
var fixedPool [fixedPoolSize][maxPathLen]byte
var fixedHeaderPool [fixedPoolSize]string
var fixedPoolIdx int = -1

var mutex sync.Mutex

// Callers must specify if they discard the returned string, and they must not care about path resolution rules.
// Additionally the max number of borrowed strings is limited, so excessive recursion/concurrency must be avoided.
//
// Performance-Driven
//   Joins paths with minimal copying and allocation. Resulting path is not 'clean' (i.e. no filepath.Clean())
// Problem
//   Profiling showed that filepath.Join() was taking 9% of total exec time. This is too high.
// Improvement
//   Total exec time has been reduced to 2%.
// Caveats
//   If there are too many outstanding calls, strings borrowed from the fixed pool will be overwritten.
//   If even a single caller stores the string forever, the 64MB dynamic pool won't be gc'd when we discard it here.
// How To Test
//   Run go pprof for the default 30s then type 'web' to get a call graph.
//   Look for an UnsafePathJoin() entry and cost. If absent, it's taking a relatively negligible amount of time.

func UnsafePathJoin(discards bool, paths ...string) string {
	// Get byte buffer and string header from the correct set of pools.
	mutex.Lock()
	var result *string
	var bytes *[maxPathLen]byte
	if !discards {
		poolIdx++
		if poolIdx == poolSize {
			pool = make([][maxPathLen]byte, poolSize)
			headerPool = make([]string, poolSize)
			poolIdx = 0
		}
		bytes = &pool[poolIdx]
		result = &headerPool[poolIdx]
	} else {
		fixedPoolIdx++
		if fixedPoolIdx == fixedPoolSize {
			fixedPoolIdx = 0
		}
		bytes = &fixedPool[fixedPoolIdx]
		result = &fixedHeaderPool[fixedPoolIdx]
	}
	mutex.Unlock()

	// Copy paths into the buffer.
	bytesIdx := 0
	for _, path := range paths {
		if len(path)+bytesIdx+1 > maxPathLen {
			panic("Path too long!")
		}
		if bytesIdx > 0 && len(path) > 0 {
			bytes[bytesIdx] = '/'
			bytesIdx++
		}
		for pathIdx := 0; pathIdx < len(path); pathIdx, bytesIdx = pathIdx+1, bytesIdx+1 {
			bytes[bytesIdx] = path[pathIdx]
		}
	}

	// Convert back to string without copying. The intention is to minimize runtime.* calls (ex: slicebytetostring).
	//TODO: confirm that headerPool avoids allocating memory for the unnamed return value.
	strHeader := (*reflect.StringHeader)(unsafe.Pointer(result))
	strHeader.Data = (uintptr)(unsafe.Pointer(bytes))
	strHeader.Len = bytesIdx
	return *result
}
