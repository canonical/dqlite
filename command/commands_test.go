package command_test

import (
	"testing"

	"github.com/dqlite/dqlite/command"
	"github.com/dqlite/go-sqlite3x"
	"github.com/golang/protobuf/proto"
)

func TestUnmarshal(t *testing.T) {
	data, err := proto.Marshal(&command.Command{
		Code:   command.Code_BEGIN,
		Params: []byte("hello"),
	})
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := command.Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Code != command.Code_BEGIN {
		t.Fatalf("expected begin code (%d), got %d", command.Code_BEGIN, cmd.Code)
	}
}

func TestUnmarshal_Error(t *testing.T) {
	cmd, err := command.Unmarshal([]byte("garbage"))
	if cmd != nil {
		t.Error("non-nil Command returned despited garbage was passed")
	}
	if err == nil {
		t.Error("nil error returned despite garbage was passed")
	}
}

func TestParamsDefaults(t *testing.T) {
	if (*command.Begin).GetTxid(nil) != "" {
		t.Errorf("expected empty Txid with nil Begin command")
	}
	if (*command.Begin).GetName(nil) != "" {
		t.Errorf("expected empty Name with nil Begin command")
	}

	if (*command.WalFrames).GetTxid(nil) != "" {
		t.Errorf("expected empty Txid with nil WalFrames command")
	}
	if (*command.WalFrames).GetPageSize(nil) != 0 {
		t.Errorf("expected zero PageSize with nil WalFrames command")
	}
	if (*command.WalFrames).GetPages(nil) != nil {
		t.Errorf("expected nil Pages with nil WalFrames command")
	}
	if (*command.WalFrames).GetTruncate(nil) != 0 {
		t.Errorf("expected zero Truncate with nil WalFrames command")
	}
	if (*command.WalFrames).GetIsCommit(nil) != 0 {
		t.Errorf("expected zero IsCommit with nil WalFrames command")
	}
	if (*command.WalFrames).GetSyncFlags(nil) != 0 {
		t.Errorf("expected zero SyngFlags with nil WalFrames command")
	}

	if (*command.WalFramesPage).GetData(nil) != nil {
		t.Errorf("expected nil Data with nil WalFramesPage")
	}
	if (*command.WalFramesPage).GetFlags(nil) != 0 {
		t.Errorf("expected zero Flags with nil WalFramesPage")
	}
	if (*command.WalFramesPage).GetNumber(nil) != 0 {
		t.Errorf("expected zero Number with nil WalFramesPage")
	}

	if (*command.End).GetTxid(nil) != "" {
		t.Errorf("expected empty Txid with nil End command")
	}

	if (*command.Undo).GetTxid(nil) != "" {
		t.Errorf("expected empty Txid with nil Undo command")
	}
}

func TestParams(t *testing.T) {
	cases := []struct {
		new   func() command.Params
		code  command.Code
		check func(*command.Command, *testing.T)
	}{
		{newBegin, command.Code_BEGIN, checkBegin},
		{newWalFrames, command.Code_WAL_FRAMES, checkWalFrames},
		{newWalFrames, command.Code_WAL_FRAMES, checkWalFrames},
		{newEnd, command.Code_END, checkEnd},
		{newUndo, command.Code_UNDO, checkUndo},
		{newCheckpoint, command.Code_CHECKPOINT, checkCheckpoint},
	}
	for _, c := range cases {
		t.Run(c.code.String(), func(t *testing.T) {
			data, err := command.Marshal(c.new())
			if err != nil {
				t.Fatal(err)
			}
			cmd, err := command.Unmarshal(data)
			if err != nil {
				t.Fatal(err)
			}
			if cmd.String() == "" {
				t.Error("expected non-empty string version fro cmd")
			}
			if cmd.GetCode() != c.code {
				t.Errorf("expected code %s, got %s", c.code, cmd.Code)
			}
			c.check(cmd, t)
		})
	}
}

func newBegin() command.Params {
	return command.NewBegin("abcd", "test")
}

func checkBegin(cmd *command.Command, t *testing.T) {
	params, err := cmd.UnmarshalBegin()
	if err != nil {
		t.Fatal(err)
	}
	if params.GetTxid() != "abcd" {
		t.Errorf(`expected Txid "abcd", got "%s"`, params.Txid)
	}
	if params.GetName() != "test" {
		t.Errorf(`expected Name "test", got "%s"`, params.Name)
	}
}

func newWalFrames() command.Params {
	size := 4096
	pages := sqlite3x.NewReplicationPages(2, size)

	for i := range pages {
		pages[i].Fill(make([]byte, 4096), 1, 1)
	}

	frames := &sqlite3x.ReplicationWalFramesParams{
		Pages:    pages,
		PageSize: size,
		Truncate: 1,
		IsCommit: 0,
	}
	return command.NewWalFrames("abcd", frames)
}

func checkWalFrames(cmd *command.Command, t *testing.T) {
	params, err := cmd.UnmarshalWalFrames()
	if err != nil {
		t.Fatal(err)
	}

	if params.GetTxid() != "abcd" {
		t.Errorf("expected Txid abcd, got %s", params.Txid)
	}
	if params.GetPageSize() != 4096 {
		t.Errorf("expected PageSize 4096, got %d", params.PageSize)
	}
	pages := params.GetPages()
	if len(pages) != 2 {
		t.Errorf("expected 2 pages, got %d", len(pages))
	}
	if size := len(pages[0].GetData()); size != 4096 {
		t.Errorf("expected page data to be 4096 bytes, got %d", size)
	}
	if flags := pages[0].GetFlags(); flags != 1 {
		t.Errorf("expected page flags to be 1, got %d", flags)
	}
	if number := pages[0].GetNumber(); number != 1 {
		t.Errorf("expected page number to be 1, got %d", number)
	}
	if params.GetTruncate() != 1 {
		t.Errorf("expected Truncate 1, got %d", params.Truncate)
	}
	if params.GetIsCommit() != 0 {
		t.Errorf("expected IsCommit 0, got %d", params.IsCommit)
	}
	if params.GetSyncFlags() != 0 {
		t.Errorf("expected SyncFlags 0, got %d", params.IsCommit)
	}
}

func newEnd() command.Params {
	return command.NewEnd("abcd")
}

func checkEnd(cmd *command.Command, t *testing.T) {
	params, err := cmd.UnmarshalEnd()
	if err != nil {
		t.Fatal(err)
	}
	if params.GetTxid() != "abcd" {
		t.Errorf(`expected Txid "abcd", got "%s"`, params.Txid)
	}
}

func newUndo() command.Params {
	return command.NewUndo("abcd")
}

func checkUndo(cmd *command.Command, t *testing.T) {
	params, err := cmd.UnmarshalUndo()
	if err != nil {
		t.Fatal(err)
	}
	if params.GetTxid() != "abcd" {
		t.Errorf(`expected Txid "abcd", got "%s"`, params.Txid)
	}
}

func newCheckpoint() command.Params {
	return command.NewCheckpoint("test")
}

func checkCheckpoint(cmd *command.Command, t *testing.T) {
	params, err := cmd.UnmarshalCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	if params.GetName() != "test" {
		t.Errorf(`expected Name "test", got "%s"`, params.Name)
	}
}
