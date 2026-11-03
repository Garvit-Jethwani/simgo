// Package viewer renders a simulation trace as a single, self-contained HTML
// file: a time-travel debugger with a space-time diagram, a scrubber, and an
// inspector showing every node's state at the selected step.
package viewer

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/Garvit-Jethwani/simgo/sim"
)

//go:embed viewer.html
var page string

const placeholder = "__SIMGO_DATA__"

// Options customizes the rendered page.
type Options struct {
	// Title is shown in the page header, for example the test name.
	Title string
	// Replay is a shell command that reproduces the run, shown with a copy
	// button.
	Replay string
}

type payload struct {
	Title  string     `json:"title"`
	Replay string     `json:"replay,omitempty"`
	Trace  *sim.Trace `json:"trace"`
}

// Write renders tr as HTML to w.
func Write(w io.Writer, tr *sim.Trace, opt Options) error {
	if tr == nil {
		return fmt.Errorf("viewer: nil trace")
	}
	title := opt.Title
	if title == "" {
		title = fmt.Sprintf("seed %d", tr.Seed)
	}
	// json.Marshal escapes <, > and &, so the data cannot close the
	// surrounding <script> element.
	data, err := json.Marshal(payload{Title: title, Replay: opt.Replay, Trace: tr})
	if err != nil {
		return fmt.Errorf("viewer: encode trace: %w", err)
	}
	head, tail, ok := strings.Cut(page, placeholder)
	if !ok {
		return fmt.Errorf("viewer: template is missing its data placeholder")
	}
	for _, part := range []string{head, string(data), tail} {
		if _, err := io.WriteString(w, part); err != nil {
			return err
		}
	}
	return nil
}

// WriteFile renders tr as HTML to path, creating parent directories.
func WriteFile(path string, tr *sim.Trace, opt Options) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := Write(f, tr, opt); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
