package command_test

import (
	"fmt"
	"testing"

	"github.com/dqlite/dqlite/command"
	"github.com/dqlite/go-sqlite3x"
)

func TestCommand_Marshal(t *testing.T) {
	cmd := command.NewBegin("abcd", "test")
	data, err := cmd.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if !(len(data) > 0) {
		t.Error("marshalling didn't return any data")
	}
}

func TestCommand_BeginString(t *testing.T) {
	cmd := command.NewBegin("abcd", "test")
	const want = "begin {txid=abcd name=test}"
	got := fmt.Sprintf("%s", cmd)
	if got != want {
		t.Errorf("expected\n%q\ngot\n%q", want, got)
	}
}

func TestCommand_BeginUnmarshal(t *testing.T) {
	data, err := command.NewBegin("abcd", "test").Marshal()
	if err != nil {
		t.Fatal(err)
	}

	cmd, err := command.Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Code != command.Begin {
		t.Errorf("expected command code %s, got %s", command.Begin, cmd.Code)
	}
	params, err := cmd.UnmarshalBegin()
	if err != nil {
		t.Fatal(err)
	}
	if params.Txid != "abcd" {
		t.Errorf("expected Txid abcd, got %s", params.Txid)
	}
	if params.Name != "test" {
		t.Errorf("expected Name test, got %s", params.Name)
	}
}

func TestCommand_WalFramesString(t *testing.T) {
	frames := &sqlite3x.ReplicationWalFramesParams{
		Pages: sqlite3x.NewReplicationPages(2, 4096),
	}
	cmd := command.NewWalFrames("abcd", frames)
	const want = "WAL frames {txid=abcd page-size=0 pages=2 truncate=0 is-end=0 sync-flags=0}"
	got := fmt.Sprintf("%s", cmd)
	if got != want {
		t.Errorf("expected\n%q\ngot\n%q", want, got)
	}
}

func TestCommand_WalFramesUnmarshal(t *testing.T) {
	frames := &sqlite3x.ReplicationWalFramesParams{
		Pages:    sqlite3x.NewReplicationPages(2, 4096),
		PageSize: 4096,
		Truncate: 1,
	}
	data, err := command.NewWalFrames("abcd", frames).Marshal()
	if err != nil {
		t.Fatal(err)
	}

	cmd, err := command.Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	params, err := cmd.UnmarshalWalFrames()
	if err != nil {
		t.Fatal(err)
	}
	if params.Txid != "abcd" {
		t.Errorf("expected Txid abcd, got %s", params.Txid)
	}
	if params.PageSize != 4096 {
		t.Errorf("expected PageSize 4096, got %d", params.PageSize)
	}
	if params.Truncate != 1 {
		t.Errorf("expected Truncate 1, got %d", params.Truncate)
	}
}

func TestCommand_UndoString(t *testing.T) {
	cmd := command.NewUndo("abcd")
	const want = "undo {txid=abcd}"
	got := fmt.Sprintf("%s", cmd)
	if got != want {
		t.Errorf("expected\n%q\ngot\n%q", want, got)
	}
}

func TestCommand_UndoUnmarshal(t *testing.T) {
	data, err := command.NewUndo("abcd").Marshal()
	if err != nil {
		t.Fatal(err)
	}

	cmd, err := command.Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Code != command.Undo {
		t.Errorf("expected command code %s, got %s", command.Undo, cmd.Code)
	}
	params, err := cmd.UnmarshalUndo()
	if err != nil {
		t.Fatal(err)
	}
	if params.Txid != "abcd" {
		t.Errorf("expected Txid abcd, got %s", params.Txid)
	}
}

func TestCommand_EndString(t *testing.T) {
	cmd := command.NewEnd("abcd")
	const want = "end {txid=abcd}"
	got := fmt.Sprintf("%s", cmd)
	if got != want {
		t.Errorf("expected\n%q\ngot\n%q", want, got)
	}
}

func TestCommand_EndUnmarshal(t *testing.T) {
	data, err := command.NewEnd("abcd").Marshal()
	if err != nil {
		t.Fatal(err)
	}

	cmd, err := command.Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Code != command.End {
		t.Errorf("expected command code %s, got %s", command.End, cmd.Code)
	}
	params, err := cmd.UnmarshalEnd()
	if err != nil {
		t.Fatal(err)
	}
	if params.Txid != "abcd" {
		t.Errorf("expected Txid abcd, got %s", params.Txid)
	}
}

func TestCommand_CheckpointString(t *testing.T) {
	cmd := command.NewCheckpoint("test")
	const want = "checkpoint {name=test}"
	got := fmt.Sprintf("%s", cmd)
	if got != want {
		t.Errorf("expected\n%q\ngot\n%q", want, got)
	}
}

func TestCommand_CheckpointUnmarshal(t *testing.T) {
	data, err := command.NewCheckpoint("test").Marshal()
	if err != nil {
		t.Fatal(err)
	}

	cmd, err := command.Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Code != command.Checkpoint {
		t.Errorf("expected command code %s, got %s", command.Checkpoint, cmd.Code)
	}
	params, err := cmd.UnmarshalCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	if params.Name != "test" {
		t.Errorf("expected Name test, got %s", params.Name)
	}
}
