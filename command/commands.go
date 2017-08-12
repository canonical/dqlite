package command

import (
	"github.com/CanonicalLtd/go-sqlite3x"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

// Params is just a protobuf message that holds the payload of a
// command.
type Params proto.Message

// NewBegin returns a new Begin protobuf message.
func NewBegin(txid string, name string) *Begin {
	return &Begin{
		Txid: txid,
		Name: name,
	}
}

// NewWalFrames returns a new WalFrames protobuf message.
func NewWalFrames(txid string, frames *sqlite3x.ReplicationWalFramesParams) Params {
	pages := make([]*WalFramesPage, len(frames.Pages))

	size := frames.PageSize
	for i := range frames.Pages {
		page := &frames.Pages[i]
		pages[i] = &WalFramesPage{}
		pages[i].Data = (*[1 << 30]byte)(page.Data())[:size:size]
		pages[i].Flags = uint32(page.Flags())
		pages[i].Number = uint32(page.Number())
	}
	return &WalFrames{
		Txid:      txid,
		PageSize:  int32(size),
		Pages:     pages,
		Truncate:  uint32(frames.Truncate),
		IsCommit:  int32(frames.IsCommit),
		SyncFlags: uint32(frames.SyncFlags),
	}
}

// NewUndo returns a new Undo protobuf message.
func NewUndo(txid string) Params {
	return &Undo{
		Txid: txid,
	}
}

// NewEnd returns a new End protobuf message.
func NewEnd(txid string) Params {
	return &End{
		Txid: txid,
	}
}

// NewCheckpoint returns a new Checkpoint protobuf message.
func NewCheckpoint(name string) Params {
	return &Checkpoint{
		Name: name,
	}
}

// CodeOf returns the command code for the given command parameters.
func CodeOf(params Params) Code {
	var code Code
	switch params.(type) {
	case *Begin:
		code = Code_BEGIN
	case *WalFrames:
		code = Code_WAL_FRAMES
	case *Undo:
		code = Code_UNDO
	case *End:
		code = Code_END
	case *Checkpoint:
		code = Code_CHECKPOINT
	}
	return code
}

// Marshal serializes a new Command with the given parameters.
func Marshal(params Params) ([]byte, error) {
	paramsData, err := proto.Marshal(params)
	if err != nil {
		return nil, errors.Wrap(err, "params")
	}
	command := &Command{
		Code:   CodeOf(params),
		Params: paramsData,
	}
	commandData, err := proto.Marshal(command)
	if err != nil {
		return nil, errors.Wrap(err, "command")
	}
	return commandData, nil
}

// Unmarshal returns a Command from the given data.
func Unmarshal(data []byte) (*Command, error) {
	command := &Command{}
	if err := proto.Unmarshal(data, command); err != nil {
		return nil, err
	}
	return command, nil
}

// UnmarshalBegin returns the Begin parameters from a command params
// payload.
func (c *Command) UnmarshalBegin() (*Begin, error) {
	params := &Begin{}
	if err := proto.Unmarshal(c.GetParams(), params); err != nil {
		return nil, err
	}
	return params, nil
}

// UnmarshalWalFrames returns the WalFrames parameters from a command
// params payload.
func (c *Command) UnmarshalWalFrames() (*WalFrames, error) {
	params := &WalFrames{}
	if err := proto.Unmarshal(c.GetParams(), params); err != nil {
		return nil, err
	}
	return params, nil
}

// UnmarshalUndo returns the Undo parameters from a command params
// payload.
func (c *Command) UnmarshalUndo() (*Undo, error) {
	params := &Undo{}
	if err := proto.Unmarshal(c.GetParams(), params); err != nil {
		return nil, err
	}
	return params, nil
}

// UnmarshalEnd returns the End parameters from a command params
// payload.
func (c *Command) UnmarshalEnd() (*End, error) {
	params := &End{}
	if err := proto.Unmarshal(c.GetParams(), params); err != nil {
		return nil, err
	}
	return params, nil
}

// UnmarshalCheckpoint returns the Checkpoint parameters from a command params
// payload.
func (c *Command) UnmarshalCheckpoint() (*Checkpoint, error) {
	params := &Checkpoint{}
	if err := proto.Unmarshal(c.GetParams(), params); err != nil {
		return nil, err
	}
	return params, nil
}
