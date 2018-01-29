package replication

// Toggles whether the FSM should panic in case of errors, or just return them.
func (f *FSM) PanicOnFailure(flag bool) {
	f.panicOnFailure = flag
}

// Replace the FSM's dir.
func (f *FSM) SetDir(dir string) {
	f.dir = dir
}
