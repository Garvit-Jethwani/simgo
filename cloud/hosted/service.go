package hosted

import (
	"context"
	"fmt"
	"sort"
)

// Service ties the API to storage: it submits jobs and reports their status.
type Service struct {
	Queue   Queue
	Results Results
	Blobs   Blobs
	// AllowLocalRepos permits local repository paths (local mode only).
	AllowLocalRepos bool
}

// Submit validates a job, stores it, and enqueues its shards.
func (s *Service) Submit(ctx context.Context, j Job) (Job, error) {
	if err := j.Normalize(s.AllowLocalRepos); err != nil {
		return Job{}, err
	}
	if err := s.Results.PutJob(ctx, j); err != nil {
		return Job{}, fmt.Errorf("store job: %w", err)
	}
	for _, sh := range j.Shards() {
		if err := s.Queue.Send(ctx, sh); err != nil {
			return Job{}, fmt.Errorf("enqueue shard %d: %w", sh.Index, err)
		}
	}
	return j, nil
}

// Group is a set of failures that are probably the same bug.
type Group struct {
	Key       string   `json:"key"`
	Invariant string   `json:"invariant"`
	Message   string   `json:"message"` // from the lowest failing seed
	Count     int      `json:"count"`
	Seeds     []uint64 `json:"seeds"` // lowest seeds first, at most 20
	TraceURL  string   `json:"trace_url,omitempty"`
	// Reproduction of the example seed, when it was measured.
	Exact   int `json:"exact_replays,omitempty"`
	Replays int `json:"replays,omitempty"`
}

// Status is a job's progress and findings.
type Status struct {
	Job          Job      `json:"job"`
	State        string   `json:"state"` // queued, running, done
	Shards       int      `json:"shards"`
	ShardsDone   int      `json:"shards_done"`
	ShardsFailed int      `json:"shards_failed"`
	SeedsRun     int      `json:"seeds_run"`
	Failures     int      `json:"failures"`
	Groups       []Group  `json:"groups"`
	Errors       []string `json:"errors,omitempty"`
}

// Status aggregates shard results and groups failures.
func (s *Service) Status(ctx context.Context, id string) (Status, error) {
	j, err := s.Results.GetJob(ctx, id)
	if err != nil {
		return Status{}, err
	}
	st := Status{Job: j, Shards: len(j.Shards()), Groups: []Group{}}
	results, err := s.Results.ShardResults(ctx, id)
	if err != nil {
		return Status{}, err
	}
	for _, r := range results {
		switch r.Status {
		case "done":
			st.ShardsDone++
			st.SeedsRun += r.SeedsRun
		case "error":
			st.ShardsFailed++
			if len(st.Errors) < 5 {
				st.Errors = append(st.Errors, fmt.Sprintf("shard %d: %s", r.Index, r.Error))
			}
		}
	}
	switch {
	case st.ShardsDone+st.ShardsFailed == st.Shards:
		st.State = "done"
	case len(results) > 0:
		st.State = "running"
	default:
		st.State = "queued"
	}

	failures, err := s.Results.Failures(ctx, id)
	if err != nil {
		return Status{}, err
	}
	st.Failures = len(failures)
	byKey := map[string]*Group{}
	for _, f := range failures { // sorted by seed
		g := byKey[f.Group]
		if g == nil {
			g = &Group{Key: f.Group, Invariant: f.Invariant, Message: f.Message}
			byKey[f.Group] = g
		}
		g.Count++
		if len(g.Seeds) < 20 {
			g.Seeds = append(g.Seeds, f.Seed)
		}
		if g.TraceURL == "" && f.TraceKey != "" && s.Blobs != nil {
			if u, err := s.Blobs.URL(ctx, f.TraceKey); err == nil {
				g.TraceURL = u
				if f.Reproduction != nil {
					g.Exact, g.Replays = f.Reproduction.Exact, f.Reproduction.Replays
				}
			}
		}
	}
	for _, g := range byKey {
		st.Groups = append(st.Groups, *g)
	}
	sort.Slice(st.Groups, func(i, j int) bool {
		if st.Groups[i].Count != st.Groups[j].Count {
			return st.Groups[i].Count > st.Groups[j].Count
		}
		return st.Groups[i].Key < st.Groups[j].Key
	})
	return st, nil
}
