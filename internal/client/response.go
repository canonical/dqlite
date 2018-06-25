package client

import (
	"fmt"
	"io"

	"github.com/CanonicalLtd/dqlite/internal/bindings"
	"github.com/pkg/errors"
)

type Server struct {
	Address string
}

func (r *Server) Read(reader io.Reader) error {
	m := Message{}

	if err := m.Read(reader); err != nil {
		errors.Wrap(err, "failed to read message")
	}

	if m.Type != bindings.ServerResponseServer {
		return fmt.Errorf("unexpected response")
	}

	r.Address = m.Body.GetString()

	return nil
}

type Welcome struct {
	HeartbeatTimeout uint64
}

func (r *Welcome) Read(reader io.Reader) error {
	m := Message{}

	if err := m.Read(reader); err != nil {
		errors.Wrap(err, "failed to read message")
	}

	if m.Type != bindings.ServerResponseWelcome {
		return fmt.Errorf("unexpected response")
	}

	r.HeartbeatTimeout = m.Body.GetUint64()

	return nil
}

type Db struct {
	ID     uint32
	unused uint32
}

func (r *Db) Read(reader io.Reader) error {
	m := Message{}

	if err := m.Read(reader); err != nil {
		errors.Wrap(err, "failed to read message")
	}

	if m.Type != bindings.ServerResponseDb {
		return fmt.Errorf("unexpected response %d", m.Type)
	}

	r.ID = m.Body.GetUint32()
	r.unused = m.Body.GetUint32()

	return nil
}

type Stmt struct {
	Db uint32
	ID uint32
}

func (r *Stmt) Read(reader io.Reader) error {
	m := Message{}

	if err := m.Read(reader); err != nil {
		errors.Wrap(err, "failed to read message")
	}

	if m.Type != bindings.ServerResponseStmt {
		return fmt.Errorf("unexpected response %d", m.Type)
	}

	r.Db = m.Body.GetUint32()
	r.ID = m.Body.GetUint32()

	return nil
}

type Result struct {
	LastInsertedID uint64
	RowsAffected   uint64
}

func (r *Result) Read(reader io.Reader) error {
	m := Message{}

	if err := m.Read(reader); err != nil {
		errors.Wrap(err, "failed to read message")
	}

	if m.Type != bindings.ServerResponseResult {
		return fmt.Errorf("unexpected response %d", m.Type)
	}

	r.LastInsertedID = m.Body.GetUint64()
	r.RowsAffected = m.Body.GetUint64()

	return nil
}

type Rows struct {
	unused uint64
}

func (r *Rows) Read(reader io.Reader) error {
	m := Message{}

	if err := m.Read(reader); err != nil {
		errors.Wrap(err, "failed to read message")
	}

	if m.Type != bindings.ServerResponseRows {
		return fmt.Errorf("unexpected response %d", m.Type)
	}

	r.unused = m.Body.GetUint64()

	return nil
}

type Empty struct {
	unused uint64
}

func (r *Empty) Read(reader io.Reader) error {
	m := Message{}

	if err := m.Read(reader); err != nil {
		errors.Wrap(err, "failed to read message")
	}

	if m.Type != bindings.ServerResponseEmpty {
		return fmt.Errorf("unexpected response %d", m.Type)
	}

	r.unused = m.Body.GetUint64()

	return nil
}
