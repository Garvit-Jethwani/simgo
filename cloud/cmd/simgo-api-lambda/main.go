// Command simgo-api-lambda serves the hosted API from AWS Lambda behind a
// function URL. It reads SIMGO_QUEUE_URL, SIMGO_BUCKET, SIMGO_TABLE and
// SIMGO_API_TOKEN from its environment.
package main

import (
	"context"
	"encoding/base64"
	"net/http"
	"os"
	"strings"

	"github.com/Garvit-Jethwani/simgo/cloud/awsstore"
	"github.com/Garvit-Jethwani/simgo/cloud/hosted"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func main() {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic(err)
	}
	svc := &hosted.Service{
		Queue:   &awsstore.Queue{Client: sqs.NewFromConfig(cfg), URL: os.Getenv("SIMGO_QUEUE_URL")},
		Blobs:   awsstore.NewBlobs(s3.NewFromConfig(cfg), os.Getenv("SIMGO_BUCKET")),
		Results: &awsstore.Results{Client: dynamodb.NewFromConfig(cfg), Table: os.Getenv("SIMGO_TABLE")},
	}
	h := (&hosted.API{Service: svc, Token: os.Getenv("SIMGO_API_TOKEN")}).Handler()
	lambda.Start(adapt(h))
}

// adapt turns an http.Handler into a Lambda function URL handler.
func adapt(h http.Handler) func(context.Context, events.LambdaFunctionURLRequest) (events.LambdaFunctionURLResponse, error) {
	return func(ctx context.Context, ev events.LambdaFunctionURLRequest) (events.LambdaFunctionURLResponse, error) {
		body := ev.Body
		if ev.IsBase64Encoded {
			b, err := base64.StdEncoding.DecodeString(body)
			if err != nil {
				return events.LambdaFunctionURLResponse{StatusCode: http.StatusBadRequest}, nil
			}
			body = string(b)
		}
		url := ev.RawPath
		if ev.RawQueryString != "" {
			url += "?" + ev.RawQueryString
		}
		req, err := http.NewRequestWithContext(ctx, ev.RequestContext.HTTP.Method, url, strings.NewReader(body))
		if err != nil {
			return events.LambdaFunctionURLResponse{StatusCode: http.StatusBadRequest}, nil
		}
		for k, v := range ev.Headers {
			req.Header.Set(k, v)
		}
		rw := &responseWriter{header: http.Header{}, code: http.StatusOK}
		h.ServeHTTP(rw, req)
		headers := map[string]string{}
		for k, v := range rw.header {
			headers[k] = strings.Join(v, ", ")
		}
		return events.LambdaFunctionURLResponse{StatusCode: rw.code, Headers: headers, Body: rw.body.String()}, nil
	}
}

type responseWriter struct {
	header http.Header
	code   int
	body   strings.Builder
	wrote  bool
}

func (w *responseWriter) Header() http.Header { return w.header }
func (w *responseWriter) Write(b []byte) (int, error) {
	w.wrote = true
	return w.body.Write(b)
}
func (w *responseWriter) WriteHeader(code int) {
	if !w.wrote {
		w.code = code
		w.wrote = true
	}
}
