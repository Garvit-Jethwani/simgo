// Package hosted is the core of SimGo's hosted exploration service: jobs,
// shards, the worker that runs them, and the HTTP API. It has no cloud
// dependencies; storage and queueing are interfaces with in-memory
// implementations here and AWS implementations in package awsstore.
//
// A job asks for many seeds of one test at one commit:
//
//	repo=https://github.com/you/svc ref=3f2a1c package=./raft test=TestSafety seeds=1000000
//
// It is split into shards (for example 1,000 seeds each) that workers pull
// from a queue. A worker checks out the commit, runs
//
//	go test -run '^TestSafety$' ./raft
//
// with SIMGO_FIRST_SEED and SIMGO_SEEDS set to its shard, reads the
// SIMGO_REPORT records, uploads failing traces, and stores the failures.
// Failures are grouped by invariant and message shape, so one bug found by
// 300 seeds shows up once.
package hosted

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/Garvit-Jethwani/simgo/simtest"
)

// Limits on a single job.
const (
	MaxSeedsPerJob   = 10_000_000
	DefaultShardSize = 1000
	MaxShardSize     = 100_000
)

// Job is a request to explore seeds of one test at one commit.
type Job struct {
	ID        string    `json:"id"`
	Repo      string    `json:"repo"`    // git URL (https) or, in local mode, a path
	Ref       string    `json:"ref"`     // commit SHA, branch or tag
	Package   string    `json:"package"` // package directory, such as ./examples/lease
	Test      string    `json:"test"`    // top-level test name, such as TestSafety
	FirstSeed uint64    `json:"first_seed"`
	Seeds     int       `json:"seeds"`
	ShardSize int       `json:"shard_size"`
	Created   time.Time `json:"created"`
}

// Shard is the unit of work a worker runs.
type Shard struct {
	Job       Job    `json:"job"`
	Index     int    `json:"index"`
	FirstSeed uint64 `json:"first_seed"`
	Seeds     int    `json:"seeds"`
}

// ShardResult records the outcome of one shard.
type ShardResult struct {
	JobID    string    `json:"job_id"`
	Index    int       `json:"index"`
	Status   string    `json:"status"` // "done" or "error"
	Error    string    `json:"error,omitempty"`
	SeedsRun int       `json:"seeds_run"`
	Failures int       `json:"failures"`
	Duration float64   `json:"duration_seconds"`
	Finished time.Time `json:"finished"`
}

// Failure is one failing seed found by a worker.
type Failure struct {
	JobID        string                `json:"job_id"`
	Group        string                `json:"group"`
	Seed         uint64                `json:"seed"`
	Invariant    string                `json:"invariant"`
	Message      string                `json:"message"`
	AtNs         int64                 `json:"at_ns"`
	Fingerprint  string                `json:"fingerprint"`
	TraceKey     string                `json:"trace_key,omitempty"`
	Reproduction *simtest.Reproduction `json:"reproduction,omitempty"`
}

var (
	testName = regexp.MustCompile(`^Test[A-Za-z0-9_]*$`)
	pkgPath  = regexp.MustCompile(`^\./[A-Za-z0-9_./-]*$`)
	refName  = regexp.MustCompile(`^[A-Za-z0-9_./-]{1,200}$`)
)

// ErrInvalid wraps job validation errors.
var ErrInvalid = errors.New("invalid job")

// Normalize fills defaults and validates a job. Every field ends up in a
// command line or a storage key, so validation is strict. allowLocal permits
// local repository paths (local mode only).
func (j *Job) Normalize(allowLocal bool) error {
	invalid := func(format string, a ...any) error {
		return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, a...))
	}
	switch {
	case strings.HasPrefix(j.Repo, "https://"):
		if strings.ContainsAny(j.Repo, " \t\n'\"`$;|&<>") {
			return invalid("repo contains forbidden characters")
		}
	case allowLocal && j.Repo != "" && !strings.HasPrefix(j.Repo, "-"):
	default:
		return invalid("repo must be an https:// git URL")
	}
	if j.Ref == "" {
		j.Ref = "HEAD"
	}
	if !refName.MatchString(j.Ref) || strings.HasPrefix(j.Ref, "-") || strings.Contains(j.Ref, "..") {
		return invalid("ref %q is not a valid commit, branch or tag", j.Ref)
	}
	if j.Package == "" {
		j.Package = "."
	}
	if j.Package != "." && (!pkgPath.MatchString(j.Package) || strings.Contains(j.Package, "..")) {
		return invalid("package must be a relative directory such as ./internal/raft")
	}
	if !testName.MatchString(j.Test) {
		return invalid("test must be a top-level test name such as TestSafety")
	}
	if j.FirstSeed == 0 {
		j.FirstSeed = 1
	}
	if j.Seeds <= 0 || j.Seeds > MaxSeedsPerJob {
		return invalid("seeds must be between 1 and %d", MaxSeedsPerJob)
	}
	if j.ShardSize == 0 {
		j.ShardSize = DefaultShardSize
	}
	if j.ShardSize < 1 || j.ShardSize > MaxShardSize {
		return invalid("shard_size must be between 1 and %d", MaxShardSize)
	}
	if j.ID == "" {
		j.ID = newID()
	}
	if j.Created.IsZero() {
		j.Created = time.Now().UTC()
	}
	return nil
}

// Shards splits a normalized job into shards.
func (j Job) Shards() []Shard {
	var out []Shard
	for i, first := 0, 0; first < j.Seeds; i, first = i+1, first+j.ShardSize {
		n := j.ShardSize
		if first+n > j.Seeds {
			n = j.Seeds - first
		}
		out = append(out, Shard{Job: j, Index: i, FirstSeed: j.FirstSeed + uint64(first), Seeds: n})
	}
	return out
}

func newID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return time.Now().UTC().Format("20060102") + "-" + hex.EncodeToString(b)
}

var digits = regexp.MustCompile(`[0-9]+`)

// GroupKey groups failures that are probably the same bug: same invariant,
// and the same message once numbers are masked ("job 7 charged twice" and
// "job 31 charged twice" group together).
func GroupKey(invariant, message string) string {
	first, _, _ := strings.Cut(message, "\n")
	shape := digits.ReplaceAllString(first, "#")
	sum := fnv64(invariant + "\x00" + shape)
	return fmt.Sprintf("%016x", sum)
}

func fnv64(s string) uint64 {
	const offset, prime = 14695981039346656037, 1099511628211
	h := uint64(offset)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	return h
}
