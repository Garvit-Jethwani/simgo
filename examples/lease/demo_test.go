package lease

import (
	"os"
	"testing"

	"github.com/Garvit-Jethwani/simgo/simtest"
)

// TestDemoNaiveWorker explores the buggy worker with simtest.Explore, so it
// FAILS by design and prints SimGo's failure report. It only runs with
// SIMGO_DEMO=1 (for demos and the hosted service walkthrough):
//
//	SIMGO_DEMO=1 go test -run TestDemoNaiveWorker ./examples/lease
func TestDemoNaiveWorker(t *testing.T) {
	if os.Getenv("SIMGO_DEMO") == "" {
		t.Skip("fails by design; set SIMGO_DEMO=1 to run")
	}
	simtest.Explore(t, Config(3, false), simtest.Options{Seeds: 200})
}
