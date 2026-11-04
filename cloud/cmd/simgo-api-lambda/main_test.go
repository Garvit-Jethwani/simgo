package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/Garvit-Jethwani/simgo/cloud/hosted"
	"github.com/aws/aws-lambda-go/events"
)

func TestAdapterRunsTheAPI(t *testing.T) {
	q := hosted.NewMemoryQueue()
	svc := &hosted.Service{Queue: q, Results: hosted.NewMemoryResults(), Blobs: hosted.DirBlobs{Dir: t.TempDir()}}
	handle := adapt((&hosted.API{Service: svc, Token: "tok"}).Handler())

	req := func(method, path, auth, body string, b64 bool) events.LambdaFunctionURLResponse {
		ev := events.LambdaFunctionURLRequest{RawPath: path, Body: body, IsBase64Encoded: b64, Headers: map[string]string{"authorization": auth}}
		ev.RequestContext.HTTP.Method = method
		resp, err := handle(context.Background(), ev)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	if r := req("POST", "/jobs", "Bearer nope", "{}", false); r.StatusCode != 401 {
		t.Fatalf("bad token: %d", r.StatusCode)
	}
	body := base64.StdEncoding.EncodeToString([]byte(`{"repo":"https://github.com/x/y","test":"TestX","seeds":10}`))
	r := req("POST", "/jobs", "Bearer tok", body, true)
	if r.StatusCode != 201 || r.Headers["Content-Type"] != "application/json" {
		t.Fatalf("submit: %d %v %s", r.StatusCode, r.Headers, r.Body)
	}
	var job hosted.Job
	json.Unmarshal([]byte(r.Body), &job)
	if r := req("GET", "/jobs/"+job.ID, "Bearer tok", "", false); r.StatusCode != 200 {
		t.Fatalf("status: %d %s", r.StatusCode, r.Body)
	}
	if q.Len() != 1 {
		t.Fatalf("expected 1 shard enqueued, got %d", q.Len())
	}
}
