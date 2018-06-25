package client

import (
	"io"

	"github.com/CanonicalLtd/dqlite/internal/bindings"
)

type Leader struct {
	unused uint64
}

func (r *Leader) Write(w io.Writer) error {
	message := Message{}
	message.Type = bindings.ServerRequestLeader
	message.Body.Bytes = make([]byte, messageWordSize)

	message.Body.PutUint64(r.unused)

	return message.Write(w)
}

type Open struct {
	name  string
	flags uint64
	vfs   string
}

func (r *Open) Write(w io.Writer) error {
	message := Message{}
	message.Type = bindings.ServerRequestOpen

	size := requestStringSize(r.name)
	size += messageWordSize
	size += requestStringSize(r.vfs)

	message.Body.Bytes = make([]byte, size)

	message.Body.PutString(r.name)
	message.Body.PutUint64(r.flags)
	message.Body.PutString(r.vfs)

	return message.Write(w)
}

type Prepare struct {
	db  uint64
	sql string
}

func (r *Prepare) Write(w io.Writer) error {
	message := Message{}
	message.Type = bindings.ServerRequestPrepare

	size := uint64(messageWordSize)
	size += requestStringSize(r.sql)

	message.Body.Bytes = make([]byte, size)

	message.Body.PutUint64(r.db)
	message.Body.PutString(r.sql)

	return message.Write(w)
}

type Exec struct {
	db   uint32
	stmt uint32
}

func (r *Exec) Write(w io.Writer) error {
	message := Message{}
	message.Type = bindings.ServerRequestExec

	size := uint64(messageWordSize / 2)
	size += uint64(messageWordSize / 2)

	message.Body.Bytes = make([]byte, size)

	message.Body.PutUint32(r.db)
	message.Body.PutUint32(r.stmt)

	return message.Write(w)
}

type Query struct {
	db   uint32
	stmt uint32
}

func (r *Query) Write(w io.Writer) error {
	message := Message{}
	message.Type = bindings.ServerRequestQuery

	size := uint64(messageWordSize / 2)
	size += uint64(messageWordSize / 2)

	message.Body.Bytes = make([]byte, size)

	message.Body.PutUint32(r.db)
	message.Body.PutUint32(r.stmt)

	return message.Write(w)
}

type Finalize struct {
	db   uint32
	stmt uint32
}

func (r *Finalize) Write(w io.Writer) error {
	message := Message{}
	message.Type = bindings.ServerRequestFinalize

	size := uint64(messageWordSize / 2)
	size += uint64(messageWordSize / 2)

	message.Body.Bytes = make([]byte, size)

	message.Body.PutUint32(r.db)
	message.Body.PutUint32(r.stmt)

	return message.Write(w)
}

type QuerySQL struct {
	db  uint64
	sql string
}

func (r *QuerySQL) Write(w io.Writer) error {
	message := Message{}
	message.Type = bindings.ServerRequestQuerySQL

	size := uint64(messageWordSize)
	size += requestStringSize(r.sql)

	message.Body.Bytes = make([]byte, size)

	message.Body.PutUint64(r.db)
	message.Body.PutString(r.sql)

	return message.Write(w)
}

type ExecSQL struct {
	db  uint64
	sql string
}

func (r *ExecSQL) Write(w io.Writer) error {
	message := Message{}
	message.Type = bindings.ServerRequestExecSQL

	size := uint64(messageWordSize)
	size += requestStringSize(r.sql)

	message.Body.Bytes = make([]byte, size)

	message.Body.PutUint64(r.db)
	message.Body.PutString(r.sql)

	return message.Write(w)
}

func requestStringSize(s string) uint64 {
	l := len(s)
	l++

	if (l % messageWordSize) != 0 {
		// Account for padding.
		l += messageWordSize - (l % messageWordSize)
	}

	return uint64(l)
}
