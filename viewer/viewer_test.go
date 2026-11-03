package viewer

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Garvit-Jethwani/simgo/sim"
)

type echo struct{}

type Note struct{ Text string }

func (echo) Start(env *sim.Env) {
	if env.ID() == 0 {
		env.Send(1, Note{Text: `</script><script>alert("x")</script>`})
	}
}
func (echo) Receive(env *sim.Env, from sim.NodeID, m sim.Message) {}
func (echo) Timer(env *sim.Env, name string)                      {}

func render(t *testing.T) string {
	t.Helper()
	r := sim.Run(sim.Config{Seed: 1, Nodes: 2, NewNode: func(sim.NodeID) sim.Node { return echo{} }, Duration: time.Second})
	var buf bytes.Buffer
	if err := Write(&buf, r.Trace, Options{Title: "TestX · seed 1", Replay: "SIMGO_SEED=1 go test -run '^TestX$' ."}); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func TestWriteEmbedsTraceSafely(t *testing.T) {
	out := render(t)
	if strings.Contains(out, placeholder) {
		t.Fatal("placeholder was not replaced")
	}
	// The hostile message must not be able to close the data <script> tag.
	if strings.Count(out, "</script>") != strings.Count(page, "</script>") {
		t.Fatal("trace data introduced an extra </script> tag")
	}
	m := regexp.MustCompile(`(?s)<script id="simgo-data" type="application/json">(.*?)</script>`).FindStringSubmatch(out)
	if m == nil {
		t.Fatal("data script not found")
	}
	var p payload
	if err := json.Unmarshal([]byte(m[1]), &p); err != nil {
		t.Fatalf("embedded data is not valid JSON: %v", err)
	}
	if p.Trace.Seed != 1 || len(p.Trace.Nodes) != 2 || p.Title != "TestX · seed 1" {
		t.Fatalf("embedded data mismatch: %+v", p)
	}
}

func TestPageHasNoExternalRequests(t *testing.T) {
	for _, bad := range []string{"<link ", "src=\"http", "src='http", "@import", "fetch("} {
		if strings.Contains(page, bad) {
			t.Errorf("viewer must be self-contained, found %q", bad)
		}
	}
}

func TestNilTrace(t *testing.T) {
	if err := Write(&bytes.Buffer{}, nil, Options{}); err == nil {
		t.Fatal("expected an error for a nil trace")
	}
}
