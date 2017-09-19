package logging_test

import (
	"bytes"
	"fmt"
	"log"
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/logging"
	"github.com/mpvl/subtest"
	"github.com/stretchr/testify/assert"
)

func TestLogger_Debugf(t *testing.T) {
	cases := []struct {
		title  string
		level  logging.Level
		prefix string
		format string
		values []interface{}
		output string
	}{
		{
			`same level`,
			logging.Debug,
			"",
			"hello",
			[]interface{}{},
			"[DEBUG] hello\n",
		},
		{
			`lower level`,
			logging.Trace,
			"",
			"hello",
			[]interface{}{},
			"[DEBUG] hello\n",
		},
		{
			`higher level`,
			logging.Info,
			"",
			"hello",
			[]interface{}{},
			"",
		},
		{
			`with prefix`,
			logging.Trace,
			"foo: ",
			"hello",
			[]interface{}{},
			"[DEBUG] foo: hello\n",
		},
		{
			`with values`,
			logging.Trace,
			"foo: ",
			"hello %s!",
			[]interface{}{"world"},
			"[DEBUG] foo: hello world!\n",
		},
	}
	for _, c := range cases {
		subtest.Run(t, c.title, func(t *testing.T) {
			buffer := bytes.NewBuffer(nil)
			logger := logging.New(log.New(buffer, "", 0), c.level, c.prefix)
			logger.Debugf(c.format, c.values...)
			assert.Equal(t, c.output, buffer.String())
		})
	}
}

func TestLogger_Levels(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	logger := logging.New(log.New(buffer, "", 0), logging.Trace, "")
	cases := []struct {
		level  logging.Level
		method func(string, ...interface{})
	}{
		{logging.Trace, logger.Tracef},
		{logging.Debug, logger.Debugf},
		{logging.Info, logger.Infof},
		{logging.Error, logger.Errorf},
	}
	for _, c := range cases {
		subtest.Run(t, c.level.String(), func(t *testing.T) {
			buffer.Reset()
			c.method("hi")
			assert.Equal(t, fmt.Sprintf("[%s] hi\n", c.level), buffer.String())
		})
	}
}

func TestLogger_Panicf(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	logger := logging.New(log.New(buffer, "", 0), logging.Trace, "foo: ")
	f := func() { logger.Panicf("hi") }
	assert.PanicsWithValue(t, "foo: hi", f)
}

func TestAugmentLogger(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	logger := logging.New(log.New(buffer, "", 0), logging.Trace, "foo: ")
	logger = logging.AugmentPrefix(logger, "bar: ")
	logger.Tracef("hi")
	assert.Equal(t, "[TRACE] foo: bar: hi\n", buffer.String())
}
