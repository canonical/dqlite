// +build linux
// +build 386 arm ppc s390

package replication

// Index returns the last Raft log index that was successfully applied by this
// FSM.
func (f *FSM) Index() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.index
}

func (f *FSM) saveIndex(index uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.index = index
}
