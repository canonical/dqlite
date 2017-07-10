package command

// Code is as a key for differentiating between the various
// LogCommands received by the DQLite raft state machine.
type Code int

func (c Code) String() string {
	name := "unknown"
	if int(c) >= 0 && int(c) < len(commandNames) {
		name = commandNames[c]
	}
	return name
}

// Available codes
const (
	None       Code = iota // Marker indicating no command
	Begin                  // Begin a new write transaction
	WalFrames              // Write frames to the write-ahead log
	Undo                   // Undo a write transaction
	End                    // End a write transaction
	Checkpoint             // Perform a WAL checkpoint
)

var commandNames = []string{
	None:       "none",
	Begin:      "begin",
	WalFrames:  "WAL frames",
	Undo:       "undo",
	End:        "end",
	Checkpoint: "checkpoint",
}
