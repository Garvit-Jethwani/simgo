// Command simgo-cloud runs SimGo's hosted exploration service.
//
//	simgo-cloud local  -repo . -package ./examples/wal -test TestX -seeds 5000
//	simgo-cloud worker                       (AWS: SQS + S3 + DynamoDB)
//	simgo-cloud api    -addr :8080           (AWS-backed API, for testing)
//	simgo-cloud submit -api URL -repo https://github.com/you/svc -ref SHA -package ./p -test TestX -seeds 100000
//	simgo-cloud status -api URL JOB_ID
//
// AWS commands read SIMGO_QUEUE_URL, SIMGO_BUCKET and SIMGO_TABLE, plus the
// usual AWS credential and region variables. submit and status read the
// API token from SIMGO_API_TOKEN.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Garvit-Jethwani/simgo/cloud/awsstore"
	"github.com/Garvit-Jethwani/simgo/cloud/hosted"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	switch os.Args[1] {
	case "local":
		err = runLocal(ctx, os.Args[2:])
	case "worker":
		err = runWorker(ctx, os.Args[2:])
	case "api":
		err = runAPI(ctx, os.Args[2:])
	case "submit":
		err = runSubmit(ctx, os.Args[2:])
	case "status":
		err = runStatus(ctx, os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "simgo-cloud:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: simgo-cloud <command> [flags]

commands:
  local    run a job end to end on this machine (no AWS needed)
  worker   process shards from SQS, store results in S3 and DynamoDB
  api      serve the HTTP API backed by AWS (the Lambda does this in production)
  submit   submit a job to a deployed API
  status   show a job's progress and grouped failures`)
	os.Exit(2)
}

func jobFlags(fs *flag.FlagSet) *hosted.Job {
	j := &hosted.Job{}
	fs.StringVar(&j.Repo, "repo", "", "git repository (https URL; local path allowed in local mode)")
	fs.StringVar(&j.Ref, "ref", "HEAD", "commit, branch or tag")
	fs.StringVar(&j.Package, "package", ".", "package directory, such as ./internal/raft")
	fs.StringVar(&j.Test, "test", "", "test name, such as TestSafety")
	fs.IntVar(&j.Seeds, "seeds", 1000, "number of seeds")
	fs.Uint64Var(&j.FirstSeed, "first-seed", 1, "first seed")
	fs.IntVar(&j.ShardSize, "shard-size", hosted.DefaultShardSize, "seeds per shard")
	return j
}

// ------------------------------------------------------------------ local

func runLocal(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("local", flag.ExitOnError)
	j := jobFlags(fs)
	workers := fs.Int("workers", 2, "concurrent workers")
	out := fs.String("out", "simgo-cloud-out", "directory for traces")
	fs.Parse(args)
	if j.Repo == "" {
		j.Repo = "."
	}
	if abs, err := filepath.Abs(j.Repo); err == nil && !strings.HasPrefix(j.Repo, "https://") {
		j.Repo = abs
	}

	q := hosted.NewMemoryQueue()
	svc := &hosted.Service{Queue: q, Results: hosted.NewMemoryResults(), Blobs: hosted.DirBlobs{Dir: *out}, AllowLocalRepos: true}
	job, err := svc.Submit(ctx, *j)
	if err != nil {
		return err
	}
	fmt.Printf("job %s: %d seeds of %s in %d shards, %d workers\n", job.ID, job.Seeds, job.Test, len(job.Shards()), *workers)

	w := &hosted.Worker{Queue: q, Blobs: svc.Blobs, Results: svc.Results, Log: slog.New(slog.NewTextHandler(os.Stderr, nil))}
	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, *workers)
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for q.Len() > 0 && ctx.Err() == nil {
				if _, err := w.RunOnce(ctx); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		return err
	}
	st, err := svc.Status(ctx, job.ID)
	if err != nil {
		return err
	}
	printStatus(os.Stdout, st)
	fmt.Printf("\nfinished in %v\n", time.Since(start).Round(time.Millisecond))
	return nil
}

// -------------------------------------------------------------------- AWS

func awsService(ctx context.Context) (*hosted.Service, *awsstore.Queue, error) {
	queueURL, bucket, table := os.Getenv("SIMGO_QUEUE_URL"), os.Getenv("SIMGO_BUCKET"), os.Getenv("SIMGO_TABLE")
	if queueURL == "" || bucket == "" || table == "" {
		return nil, nil, errors.New("SIMGO_QUEUE_URL, SIMGO_BUCKET and SIMGO_TABLE must be set")
	}
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, nil, err
	}
	q := &awsstore.Queue{Client: sqs.NewFromConfig(cfg), URL: queueURL}
	svc := &hosted.Service{
		Queue:   q,
		Blobs:   awsstore.NewBlobs(s3.NewFromConfig(cfg), bucket),
		Results: &awsstore.Results{Client: dynamodb.NewFromConfig(cfg), Table: table},
	}
	return svc, q, nil
}

func runWorker(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	maxTraces := fs.Int("max-traces", 20, "traces uploaded per shard")
	timeout := fs.Duration("shard-timeout", 30*time.Minute, "time limit for one shard")
	fs.Parse(args)
	svc, q, err := awsService(ctx)
	if err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	log.Info("worker started")
	w := &hosted.Worker{Queue: q, Blobs: svc.Blobs, Results: svc.Results, MaxTraces: *maxTraces, ShardTimeout: *timeout, Log: log}
	return w.Run(ctx)
}

func runAPI(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("api", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "listen address")
	fs.Parse(args)
	svc, _, err := awsService(ctx)
	if err != nil {
		return err
	}
	srv := &http.Server{Addr: *addr, Handler: (&hosted.API{Service: svc, Token: os.Getenv("SIMGO_API_TOKEN")}).Handler()}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	fmt.Println("listening on", *addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// ----------------------------------------------------------------- client

func apiRequest(ctx context.Context, method, url string, body any, out any) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, r)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+os.Getenv("SIMGO_API_TOKEN"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	return json.Unmarshal(data, out)
}

func runSubmit(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	api := fs.String("api", os.Getenv("SIMGO_API_URL"), "API base URL")
	j := jobFlags(fs)
	fs.Parse(args)
	var job hosted.Job
	if err := apiRequest(ctx, http.MethodPost, strings.TrimRight(*api, "/")+"/jobs", j, &job); err != nil {
		return err
	}
	fmt.Printf("submitted job %s: %d seeds in %d shards\n", job.ID, job.Seeds, len(job.Shards()))
	return nil
}

func runStatus(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	api := fs.String("api", os.Getenv("SIMGO_API_URL"), "API base URL")
	asJSON := fs.Bool("json", false, "print raw JSON")
	fs.Parse(args)
	if fs.NArg() != 1 {
		return errors.New("usage: simgo-cloud status [-api URL] JOB_ID")
	}
	var st hosted.Status
	if err := apiRequest(ctx, http.MethodGet, strings.TrimRight(*api, "/")+"/jobs/"+fs.Arg(0), nil, &st); err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(st)
	}
	printStatus(os.Stdout, st)
	return nil
}

func printStatus(w io.Writer, st hosted.Status) {
	fmt.Fprintf(w, "\njob %s (%s): %d/%d shards done, %d failed, %d seeds run, %d failing seeds\n",
		st.Job.ID, st.State, st.ShardsDone, st.Shards, st.ShardsFailed, st.SeedsRun, st.Failures)
	for _, e := range st.Errors {
		fmt.Fprintf(w, "  error: %s\n", firstLine(e))
	}
	if len(st.Groups) == 0 {
		fmt.Fprintln(w, "  no violations found")
		return
	}
	for i, g := range st.Groups {
		fmt.Fprintf(w, "\n  bug %d: %q in %d seeds (first: %v)\n    %s\n", i+1, g.Invariant, g.Count, g.Seeds[0], firstLine(g.Message))
		if g.Replays > 0 {
			fmt.Fprintf(w, "    reproduced exactly in %d of %d replays\n", g.Exact, g.Replays)
		}
		if g.TraceURL != "" {
			fmt.Fprintf(w, "    trace: %s\n", g.TraceURL)
		}
	}
}

func firstLine(s string) string {
	l, _, _ := strings.Cut(s, "\n")
	return l
}
