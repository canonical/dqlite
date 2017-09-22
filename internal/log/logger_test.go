package log_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/log"
	"github.com/mpvl/subtest"
	"github.com/stretchr/testify/assert"
)

func TestLogger_Debugf(t *testing.T) {
	cases := []struct {
		title  string
		level  log.Level
		format string
		values []interface{}
		output string
	}{
		{
			`same level`,
			log.Debug,
			"hello",
			[]interface{}{},
			"DEBUG hello",
		},
		{
			`lower level`,
			log.Trace,
			"hello",
			[]interface{}{},
			"DEBUG hello",
		},
		{
			`higher level`,
			log.Info,
			"hello",
			[]interface{}{},
			"",
		},
		{
			`with values`,
			log.Trace,
			"hello %s!",
			[]interface{}{"world"},
			"DEBUG hello world!",
		},
	}
	for _, c := range cases {
		subtest.Run(t, c.title, func(t *testing.T) {
			buffer := newBuffer()
			logger := log.New(buffer, c.level)
			logger.Debugf(c.format, c.values...)
			assert.Equal(t, c.output, buffer.String())
		})
	}
}

func TestLogger_Levels(t *testing.T) {
	buffer := newBuffer()
	logger := log.New(buffer, log.Trace)
	cases := []struct {
		level  log.Level
		method func(string, ...interface{})
	}{
		{log.Trace, logger.Tracef},
		{log.Debug, logger.Debugf},
		{log.Info, logger.Infof},
		{log.Error, logger.Errorf},
	}
	for _, c := range cases {
		subtest.Run(t, c.level.String(), func(t *testing.T) {
			buffer.Reset()
			c.method("hi")
			assert.Equal(t, fmt.Sprintf("%s hi", c.level), buffer.String())
		})
	}
}

func TestLogger_Panicf(t *testing.T) {
	logger := log.New(newBuffer(), log.Trace)
	f := func() { logger.Panicf("hi") }
	assert.PanicsWithValue(t, " hi", f)
}

func TestAugmentLogger(t *testing.T) {
	buffer := newBuffer()
	logger := log.New(buffer, log.Trace)
	logger = logger.Augment("foo")
	logger.Tracef("hi")
	assert.Equal(t, "TRACE foo: hi", buffer.String())
}

type buffer struct {
	bytes.Buffer
}

func newBuffer() *buffer {
	return &buffer{}
}

func (b *buffer) Output(level log.Level, message string) error {
	_, err := b.WriteString(fmt.Sprintf("%s %s", level, message))
	return err
}
