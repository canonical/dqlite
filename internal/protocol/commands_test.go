package protocol_test

import (
	"strings"
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/protocol"
	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/mpvl/subtest"
)

func TestUnmarshal_Error(t *testing.T) {
	cmd, err := protocol.UnmarshalCommand([]byte("garbage"))
	if cmd != nil {
		t.Error("non-nil Command returned despited garbage was passed")
	}
	if err == nil {
		t.Fatal("nil error returned despite garbage was passed")
	}
	if !strings.HasPrefix(err.Error(), "protobuf failure") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// Create, marshal, unmarshal and check dqlite FSM protocol commands.
func TestCommands(t *testing.T) {
	cases := []struct {
		factory func() *protocol.Command
		checker func(*protocol.Command, *testing.T)
	}{
		{newOpen, checkOpen},
		{newBegin, checkBegin},
		{newWalFrames, checkWalFrames},
		{newEnd, checkEnd},
		{newUndo, checkUndo},
		{newCheckpoint, checkCheckpoint},
	}
	for _, c := range cases {
		cmd := c.factory()
		name := cmd.Name()
		subtest.Run(t, name, func(t *testing.T) {
			// Exercise marshaling.
			data, err := protocol.MarshalCommand(cmd)
			if err != nil {
				t.Fatalf("failed to marshal %s: %v", name, err)
			}

			// Exercise unmarshaling.
			cmd, err = protocol.UnmarshalCommand(data)
			if err != nil {
				t.Fatalf("failed to unmarshal %s: %v", name, err)
			}
			c.checker(cmd, t)
		})
	}
}

func newOpen() *protocol.Command {
	return protocol.NewOpen("test")
}

func checkOpen(cmd *protocol.Command, t *testing.T) {
	params, ok := cmd.Payload.(*protocol.Command_Open)
	if !ok {
		t.Errorf("Params field is not of type protocol.Command_Open")
	}
	if params.Open.Name != "test" {
		t.Errorf(`expected Name "test", got "%s"`, params.Open.Name)
	}
}

func newBegin() *protocol.Command {
	return protocol.NewBegin(123, "test")
}

func checkBegin(cmd *protocol.Command, t *testing.T) {
	params, ok := cmd.Payload.(*protocol.Command_Begin)
	if !ok {
		t.Errorf("Params field is not of type protocol.Command_Begin")
	}
	if params.Begin.Txid != 123 {
		t.Errorf(`expected Txid 123, got "%d"`, params.Begin.Txid)
	}
	if params.Begin.Name != "test" {
		t.Errorf(`expected Name "test", got "%s"`, params.Begin.Name)
	}
}

func newWalFrames() *protocol.Command {
	size := 4096
	pages := sqlite3.NewReplicationPages(2, size)

	for i := range pages {
		pages[i].Fill(make([]byte, 4096), 1, 1)
	}

	frames := &sqlite3.ReplicationFramesParams{
		Pages:     pages,
		PageSize:  size,
		Truncate:  1,
		IsCommit:  1,
		SyncFlags: 1,
	}
	return protocol.NewFrames(123, "test.db", frames)
}

func checkWalFrames(cmd *protocol.Command, t *testing.T) {
	params, ok := cmd.Payload.(*protocol.Command_Frames)
	if !ok {
		t.Errorf("Params field is not of type protocol.Command_Frames")
	}
	if params.Frames.Txid != 123 {
		t.Errorf("expected Txid 123, got %d", params.Frames.Txid)
	}
	if params.Frames.PageSize != 4096 {
		t.Errorf("expected PageSize 4096, got %d", params.Frames.PageSize)
	}
	pages := params.Frames.Pages
	if len(pages) != 2 {
		t.Errorf("expected 2 pages, got %d", len(pages))
	}
	if size := len(pages[0].Data); size != 4096 {
		t.Errorf("expected page data to be 4096 bytes, got %d", size)
	}
	if flags := pages[0].Flags; flags != 1 {
		t.Errorf("expected page flags to be 1, got %d", flags)
	}
	if number := pages[0].Number; number != 1 {
		t.Errorf("expected page number to be 1, got %d", number)
	}
	if params.Frames.Truncate != 1 {
		t.Errorf("expected Truncate 1, got %d", params.Frames.Truncate)
	}
	if params.Frames.IsCommit != 1 {
		t.Errorf("expected IsCommit 1, got %d", params.Frames.IsCommit)
	}
	if params.Frames.SyncFlags != 1 {
		t.Errorf("expected SyncFlags 1, got %d", params.Frames.IsCommit)
	}
	if params.Frames.Filename != "test.db" {
		t.Errorf("expected Filename test.db, got %s", params.Frames.Filename)
	}
}

func newEnd() *protocol.Command {
	return protocol.NewEnd(123)
}

func checkEnd(cmd *protocol.Command, t *testing.T) {
	params, ok := cmd.Payload.(*protocol.Command_End)
	if !ok {
		t.Errorf("Params field is not of type protocol.Command_WalFrames")
	}
	if params.End.Txid != 123 {
		t.Errorf(`expected Txid 123, got "%d"`, params.End.Txid)
	}
}

func newUndo() *protocol.Command {
	return protocol.NewUndo(123)
}

func checkUndo(cmd *protocol.Command, t *testing.T) {
	params, ok := cmd.Payload.(*protocol.Command_Undo)
	if !ok {
		t.Errorf("Params field is not of type protocol.Command_Undo")
	}
	if params.Undo.Txid != 123 {
		t.Errorf(`expected Txid 123, got "%d"`, params.Undo.Txid)
	}
}

func newCheckpoint() *protocol.Command {
	return protocol.NewCheckpoint("test")
}

func checkCheckpoint(cmd *protocol.Command, t *testing.T) {
	params, ok := cmd.Payload.(*protocol.Command_Checkpoint)
	if !ok {
		t.Errorf("Params field is not of type protocol.Command_Checkpoint")
	}
	if params.Checkpoint.Name != "test" {
		t.Errorf(`expected Name "test", got "%s"`, params.Checkpoint.Name)
	}
}
