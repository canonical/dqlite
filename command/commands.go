package command

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/dqlite/go-sqlite3x"
	"github.com/pkg/errors"
)

// NewBegin returns a new Command that instructs the dqlite FSM
// to begin a new write transaction.
func NewBegin(txid string, name string) *Command {
	params := &beginParams{
		Txid: txid,
		Name: name,
	}
	return newCommand(Begin, params)
}

// NewWalFrames creates a command to write new WAL frames.
func NewWalFrames(txid string, frames *sqlite3x.ReplicationWalFramesParams) *Command {
	pages := make([]walFramesPage, len(frames.Pages))

	size := frames.PageSize
	for i, page := range frames.Pages {
		data := (*[1 << 30]byte)(page.Data())[:size:size]
		pages[i].Data = base64.StdEncoding.EncodeToString(data)
		pages[i].Flags = page.Flags()
		pages[i].Number = page.Number()
	}
	params := &walFramesParams{
		Txid:      txid,
		PageSize:  frames.PageSize,
		Pages:     pages,
		Truncate:  frames.Truncate,
		IsCommit:  frames.IsCommit,
		SyncFlags: frames.SyncFlags,
	}
	return newCommand(WalFrames, params)
}

// NewUndo creates a command to undo any WAL changes.
func NewUndo(txid string) *Command {
	params := &undoParams{
		Txid: txid,
	}
	return newCommand(Undo, params)
}

// NewEnd creates a command to finish a WAL write transaction
// and release the relevant locks.
func NewEnd(txid string) *Command {
	params := &endParams{
		Txid: txid,
	}
	return newCommand(End, params)
}

// NewCheckpoint creates a command to checkpoint the WAL.
func NewCheckpoint(name string) *Command {
	params := &checkpointParams{
		Name: name,
	}
	return newCommand(Checkpoint, params)
}

// Unmarshal the given data into a new Command instance.
func Unmarshal(data []byte) (*Command, error) {
	command := &Command{}
	err := json.Unmarshal(data, command)
	if err != nil {
		return nil, err
	}
	return command, nil
}

// Command captures data for a single dqlite FSM raft log entry.
type Command struct {
	Code   Code
	Params interface{}
}

func (c *Command) String() string {
	params := c.Params.(Params)
	return fmt.Sprintf("%s %s", commandNames[c.Code], params.String())
}

// Marshal serializes the command into bytes.
func (c *Command) Marshal() ([]byte, error) {
	bytes, err := json.Marshal(c)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to marshal %s command", c.String()))
	}
	return bytes, nil
}

func (c *Command) UnmarshalBegin() (*beginParams, error) {
	params := &beginParams{}
	if err := c.unmarshalParams(params); err != nil {
		return nil, err
	}
	return params, nil
}

func (c *Command) UnmarshalWalFrames() (*walFramesParams, error) {
	params := &walFramesParams{}
	if err := c.unmarshalParams(params); err != nil {
		return nil, err
	}
	return params, nil
}

func (c *Command) UnmarshalUndo() (*undoParams, error) {
	params := &undoParams{}
	if err := c.unmarshalParams(params); err != nil {
		return nil, err
	}
	return params, nil
}

func (c *Command) UnmarshalEnd() (*endParams, error) {
	params := &endParams{}
	if err := c.unmarshalParams(params); err != nil {
		return nil, err
	}
	return params, nil
}

func (c *Command) UnmarshalCheckpoint() (*checkpointParams, error) {
	params := &checkpointParams{}
	if err := c.unmarshalParams(params); err != nil {
		return nil, err
	}
	return params, nil
}

func (c *Command) unmarshalParams(params interface{}) error {
	data, err := json.Marshal(c.Params)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, params); err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to unmarshal params for %s", c.Code))
	}
	return nil
}

// Params is the interface that command parameters need to
// implement.
type Params interface {
	String() string
}

func newCommand(code Code, params Params) *Command {
	return &Command{
		Code:   code,
		Params: params,
	}
}

type beginParams struct {
	Txid string
	Name string
}

func (p *beginParams) String() string {
	return fmt.Sprintf("{txid=%s name=%s}", p.Txid, p.Name)
}

type walFramesPage struct {
	Data   string
	Flags  uint
	Number uint
}

type walFramesParams struct {
	Txid      string
	PageSize  int
	Pages     []walFramesPage
	Truncate  uint
	IsCommit  int
	SyncFlags int
}

func (p *walFramesParams) String() string {
	return fmt.Sprintf(
		"{txid=%s page-size=%d pages=%d truncate=%d is-end=%d sync-flags=%d}",
		p.Txid, p.PageSize, len(p.Pages), p.Truncate, p.IsCommit, p.SyncFlags)
}

type undoParams struct {
	Txid string
}

func (p *undoParams) String() string {
	return fmt.Sprintf("{txid=%s}", p.Txid)
}

type endParams struct {
	Txid string
}

func (p *endParams) String() string {
	return fmt.Sprintf("{txid=%s}", p.Txid)
}

type checkpointParams struct {
	Name string
}

func (p *checkpointParams) String() string {
	return fmt.Sprintf("{name=%s}", p.Name)
}
