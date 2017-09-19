package commands_test

import (
	"strings"
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/commands"
	"github.com/CanonicalLtd/go-sqlite3x"
	"github.com/mpvl/subtest"
)

func TestUnmarshal_Error(t *testing.T) {
	cmd, err := commands.Unmarshal([]byte("garbage"))
	if cmd != nil {
		t.Error("non-nil Command returned despited garbage was passed")
	}
	if err == nil {
		t.Fatal("nil error returned despite garbage was passed")
	}
	if !strings.HasPrefix(err.Error(), "corrupted command data") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// Create, marshal, unmarshal and check dqlite FSM commands.
func TestCommands(t *testing.T) {
	cases := []struct {
		factory func() *commands.Command
		checker func(*commands.Command, *testing.T)
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
			data, err := commands.Marshal(cmd)
			if err != nil {
				t.Fatalf("failed to marshal %s: %v", name, err)
			}

			// Exercise unmarshaling.
			cmd, err = commands.Unmarshal(data)
			if err != nil {
				t.Fatalf("failed to unmarshal %s: %v", name, err)
			}
			c.checker(cmd, t)
		})
	}
}

func newOpen() *commands.Command {
	return commands.NewOpen("test")
}

func checkOpen(cmd *commands.Command, t *testing.T) {
	params, ok := cmd.Params.(*commands.Command_Open)
	if !ok {
		t.Errorf("Params field is not of type commands.Command_Open")
	}
	if params.Open.Name != "test" {
		t.Errorf(`expected Name "test", got "%s"`, params.Open.Name)
	}
}

func newBegin() *commands.Command {
	return commands.NewBegin("abcd", "test")
}

func checkBegin(cmd *commands.Command, t *testing.T) {
	params, ok := cmd.Params.(*commands.Command_Begin)
	if !ok {
		t.Errorf("Params field is not of type commands.Command_Begin")
	}
	if params.Begin.Txid != "abcd" {
		t.Errorf(`expected Txid "abcd", got "%s"`, params.Begin.Txid)
	}
	if params.Begin.Name != "test" {
		t.Errorf(`expected Name "test", got "%s"`, params.Begin.Name)
	}
}

func newWalFrames() *commands.Command {
	size := 4096
	pages := sqlite3x.NewReplicationPages(2, size)

	for i := range pages {
		pages[i].Fill(make([]byte, 4096), 1, 1)
	}

	frames := &sqlite3x.ReplicationWalFramesParams{
		Pages:     pages,
		PageSize:  size,
		Truncate:  1,
		IsCommit:  1,
		SyncFlags: 1,
	}
	return commands.NewWalFrames("abcd", frames)
}

func checkWalFrames(cmd *commands.Command, t *testing.T) {
	params, ok := cmd.Params.(*commands.Command_WalFrames)
	if !ok {
		t.Errorf("Params field is not of type commands.Command_WalFrames")
	}
	if params.WalFrames.Txid != "abcd" {
		t.Errorf("expected Txid abcd, got %s", params.WalFrames.Txid)
	}
	if params.WalFrames.PageSize != 4096 {
		t.Errorf("expected PageSize 4096, got %d", params.WalFrames.PageSize)
	}
	pages := params.WalFrames.Pages
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
	if params.WalFrames.Truncate != 1 {
		t.Errorf("expected Truncate 1, got %d", params.WalFrames.Truncate)
	}
	if params.WalFrames.IsCommit != 1 {
		t.Errorf("expected IsCommit 1, got %d", params.WalFrames.IsCommit)
	}
	if params.WalFrames.SyncFlags != 1 {
		t.Errorf("expected SyncFlags 1, got %d", params.WalFrames.IsCommit)
	}
}

func newEnd() *commands.Command {
	return commands.NewEnd("abcd")
}

func checkEnd(cmd *commands.Command, t *testing.T) {
	params, ok := cmd.Params.(*commands.Command_End)
	if !ok {
		t.Errorf("Params field is not of type commands.Command_WalFrames")
	}
	if params.End.Txid != "abcd" {
		t.Errorf(`expected Txid "abcd", got "%s"`, params.End.Txid)
	}
}

func newUndo() *commands.Command {
	return commands.NewUndo("abcd")
}

func checkUndo(cmd *commands.Command, t *testing.T) {
	params, ok := cmd.Params.(*commands.Command_Undo)
	if !ok {
		t.Errorf("Params field is not of type commands.Command_Undo")
	}
	if params.Undo.Txid != "abcd" {
		t.Errorf(`expected Txid "abcd", got "%s"`, params.Undo.Txid)
	}
}

func newCheckpoint() *commands.Command {
	return commands.NewCheckpoint("test")
}

func checkCheckpoint(cmd *commands.Command, t *testing.T) {
	params, ok := cmd.Params.(*commands.Command_Checkpoint)
	if !ok {
		t.Errorf("Params field is not of type commands.Command_Checkpoint")
	}
	if params.Checkpoint.Name != "test" {
		t.Errorf(`expected Name "test", got "%s"`, params.Checkpoint.Name)
	}
}
