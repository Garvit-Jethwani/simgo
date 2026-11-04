package httpkv

import (
	"os"
	"testing"

	"github.com/Garvit-Jethwani/simgo/simtest"
	"github.com/Garvit-Jethwani/simgo/x/simproc"
)

// TestDemoAsyncReplication explores the buggy service with simproc.Explore,
// so it FAILS by design and prints the failure report with reproduction
// rates. It only runs with SIMGO_DEMO=1:
//
//	SIMGO_DEMO=1 go test -run TestDemoAsyncReplication ./examples/httpkv
func TestDemoAsyncReplication(t *testing.T) {
	if os.Getenv("SIMGO_DEMO") == "" {
		t.Skip("fails by design; set SIMGO_DEMO=1 to run")
	}
	simproc.Explore(t, Config(AsyncReplication), simproc.Options{Options: simtest.Options{Seeds: 40}})
}
