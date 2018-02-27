// +build linux
// +build amd64 ppc64 ppc64le arm64 s390x

package replication

import "sync/atomic"

// Index returns the last Raft log index that was successfully applied by this
// FSM.
func (f *FSM) Index() uint64 {
	return atomic.LoadUint64(&f.index)
}

func (f *FSM) saveIndex(index uint64) {
	atomic.StoreUint64(&f.index, index)
}
