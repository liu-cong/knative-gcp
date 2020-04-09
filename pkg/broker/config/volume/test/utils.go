package test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/google/knative-gcp/pkg/broker/config"
)

func CreateConfigFile(tb testing.TB, data *config.TargetsConfig) (path string, cleanup func()) {
	b, err := proto.Marshal(data)
	if err != nil {
		tb.Fatalf("Failed to marshal data: %v", err)
	}
	dir, err := ioutil.TempDir("", "configtest-*")
	if err != nil {
		tb.Fatalf("unexpected error from creating temp dir: %v", err)
	}
	tmp, err := ioutil.TempFile(dir, "test-*")
	if err != nil {
		tb.Fatalf("unexpected error from creating config file: %v", err)
	}
	if _, err := tmp.Write(b); err != nil {
		tb.Fatalf("unexpected error from writing config file: %v", err)
	}
	if err := tmp.Close(); err != nil {
		tb.Fatalf("unexpected error from closing config file: %v", err)
	}

	return tmp.Name(), func() { os.RemoveAll(dir) }
}
