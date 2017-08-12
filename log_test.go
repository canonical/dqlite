package dqlite_test

import (
	"bytes"
	"testing"

	"github.com/CanonicalLtd/dqlite"
	"github.com/hashicorp/logutils"
)

func TestLevelFilterWithOrigin_Write(t *testing.T) {
	writer := bytes.NewBuffer(nil)
	filter := &dqlite.LevelFilterWithOrigin{}
	filter.Writer = writer
	filter.Levels = []logutils.LogLevel{"DEBUG", "INFO"}
	filter.MinLevel = "INFO"
	filter.Origins = []string{"foo"}

	cases := []struct {
		origins []string
		message string
		written bool
	}{
		{[]string{"foo"}, "[INFO] foo: hello", true},
		{[]string{"foo"}, "[DEBUG] foo: hello", false},
		{[]string{"foo"}, "[INFO] bar: hello", false},
		{[]string{"foo"}, "foo: hello", true},
		{[]string{"foo"}, "hello", true},
		{nil, "[INFO] bar: hello", true},
	}

	for _, c := range cases {
		t.Run(c.message, func(t *testing.T) {
			defer writer.Reset()
			filter.SetOrigins(c.origins)

			filter.Write([]byte(c.message))

			want := ""
			if c.written {
				want = c.message
			}
			if got := writer.String(); got != want {
				t.Errorf("got %#v, wanted %#v", got, want)
			}
		})
	}
}
