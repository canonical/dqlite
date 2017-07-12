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

func TestParams(t *testing.T) {
	cases := []struct {
		new   func() command.Params
		code  command.Code
		check func(*command.Command, *testing.T)
	}{
		{newBegin, command.Code_BEGIN, checkBegin},
		{newWalFrames, command.Code_WAL_FRAMES, checkWalFrames},
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
			if cmd.Code != c.code {
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

	if params.Txid != "abcd" {
		t.Errorf(`expected Txid "abcd", got "%s"`, params.Txid)
	}
	if params.Name != "test" {
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
	}
	return command.NewWalFrames("abcd", frames)
}

func checkWalFrames(cmd *command.Command, t *testing.T) {
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
