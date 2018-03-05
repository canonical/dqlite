package protocol

import (
	"reflect"
	"strings"

	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

// MarshalCommand marshals a dqlite FSM command.
func MarshalCommand(command *Command) ([]byte, error) {
	return proto.Marshal(command)
}

// UnmarshalCommand unmarshals a dqlite FSM command.
func UnmarshalCommand(data []byte) (*Command, error) {
	command := &Command{}
	if err := proto.Unmarshal(data, command); err != nil {
		return nil, errors.Wrap(err, "protobuf failure")
	}
	return command, nil
}

// NewOpen returns a new Command with Open parameters.
func NewOpen(name string) *Command {
	params := &Command_Open{Open: &Open{Name: name}}
	return newCommand(params)
}

// NewBegin returns a new Command with Begin parameters.
func NewBegin(txid uint64, name string) *Command {
	params := &Command_Begin{Begin: &Begin{Txid: txid, Name: name}}
	return newCommand(params)
}

// NewFrames returns a new WalFrames protobuf message.
func NewFrames(txid uint64, filename string, frames *sqlite3.ReplicationFramesParams) *Command {
	pages := make([]*FramesPage, len(frames.Pages))

	size := frames.PageSize
	for i := range frames.Pages {
		page := &frames.Pages[i]
		pages[i] = &FramesPage{}
		pages[i].Data = page.Data()
		pages[i].Flags = uint32(page.Flags())
		pages[i].Number = uint32(page.Number())
	}
	params := &Command_Frames{Frames: &Frames{
		Txid:      txid,
		PageSize:  int32(size),
		Pages:     pages,
		Truncate:  uint32(frames.Truncate),
		IsCommit:  int32(frames.IsCommit),
		SyncFlags: uint32(frames.SyncFlags),
		Filename:  filename,
	}}
	return newCommand(params)
}

// NewUndo returns a new Undo protobuf message.
func NewUndo(txid uint64) *Command {
	params := &Command_Undo{Undo: &Undo{
		Txid: txid,
	}}
	return newCommand(params)
}

// NewEnd returns a new End protobuf message.
func NewEnd(txid uint64) *Command {
	params := &Command_End{End: &End{
		Txid: txid,
	}}
	return newCommand(params)
}

// NewCheckpoint returns a new Checkpoint protobuf message.
func NewCheckpoint(name string) *Command {
	params := &Command_Checkpoint{Checkpoint: &Checkpoint{
		Name: name,
	}}
	return newCommand(params)
}

func newCommand(payload isCommand_Payload) *Command {
	return &Command{Payload: payload}
}

// Name returns a human readable name for the command, based on its Params
// type.
func (c *Command) Name() string {
	typeName := reflect.TypeOf(c.Payload).Elem().String()
	return strings.ToLower(strings.Replace(typeName, "protocol.Command_", "", 1))
}
