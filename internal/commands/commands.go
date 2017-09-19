package commands

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/CanonicalLtd/go-sqlite3x"
	"github.com/golang/protobuf/proto"
)

// Marshal a dqlite FSM command.
func Marshal(command *Command) ([]byte, error) {
	return proto.Marshal(command)
}

// Unmarshal a dqlite FSM command.
func Unmarshal(data []byte) (*Command, error) {
	command := &Command{}
	if err := proto.Unmarshal(data, command); err != nil {
		return nil, fmt.Errorf("corrupted command data: %s", err)
	}
	return command, nil
}

// NewOpen returns a new Command with Open parameters.
func NewOpen(name string) *Command {
	params := &Command_Open{Open: &Open{Name: name}}
	return newCommand(params)
}

// NewBegin returns a new Command with Begin parameters.
func NewBegin(txid string, name string) *Command {
	params := &Command_Begin{Begin: &Begin{Txid: txid, Name: name}}
	return newCommand(params)
}

// NewWalFrames returns a new WalFrames protobuf message.
func NewWalFrames(txid string, frames *sqlite3x.ReplicationWalFramesParams) *Command {
	pages := make([]*WalFramesPage, len(frames.Pages))

	size := frames.PageSize
	for i := range frames.Pages {
		page := &frames.Pages[i]
		pages[i] = &WalFramesPage{}
		pages[i].Data = (*[1 << 30]byte)(page.Data())[:size:size]
		pages[i].Flags = uint32(page.Flags())
		pages[i].Number = uint32(page.Number())
	}
	params := &Command_WalFrames{WalFrames: &WalFrames{
		Txid:      txid,
		PageSize:  int32(size),
		Pages:     pages,
		Truncate:  uint32(frames.Truncate),
		IsCommit:  int32(frames.IsCommit),
		SyncFlags: uint32(frames.SyncFlags),
	}}
	return newCommand(params)
}

// NewUndo returns a new Undo protobuf message.
func NewUndo(txid string) *Command {
	params := &Command_Undo{Undo: &Undo{
		Txid: txid,
	}}
	return newCommand(params)
}

// NewEnd returns a new End protobuf message.
func NewEnd(txid string) *Command {
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

func newCommand(params isCommand_Params) *Command {
	return &Command{Params: params}
}

// Name returns a human readable name for the command, based on its Params
// type.
func (c *Command) Name() string {
	typeName := reflect.TypeOf(c.Params).Elem().String()
	return strings.Replace(typeName, "commands.Command_", "", 1)
}
