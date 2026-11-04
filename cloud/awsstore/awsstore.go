// Package awsstore implements the hosted service's storage on AWS:
// SQS for the shard queue, S3 for traces, and DynamoDB for results.
//
// DynamoDB uses a single table with a string partition key "pk" and sort key
// "sk":
//
//	pk=JOB#<id>  sk=META               the job
//	pk=JOB#<id>  sk=SHARD#000042       a shard result
//	pk=JOB#<id>  sk=FAIL#<group>#<seed> a failure
//
// Each item stores its value as JSON in "data" and an "expires" epoch used
// as the table's TTL attribute.
package awsstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Garvit-Jethwani/simgo/cloud/hosted"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// ------------------------------------------------------------------- SQS

// SQSAPI is the subset of the SQS client the queue uses.
type SQSAPI interface {
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, opts ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

// Queue is a hosted.Queue on SQS. Shards not deleted within the queue's
// visibility timeout are redelivered; configure a dead-letter queue to stop
// retrying shards that keep failing.
type Queue struct {
	Client SQSAPI
	URL    string
	// WaitSeconds is the long-poll wait (max 20). Default 20.
	WaitSeconds int32
}

func (q *Queue) Send(ctx context.Context, s hosted.Shard) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	_, err = q.Client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(q.URL), MessageBody: aws.String(string(b))})
	return err
}

func (q *Queue) Receive(ctx context.Context) (*hosted.Delivery, error) {
	wait := q.WaitSeconds
	if wait == 0 {
		wait = 20
	}
	out, err := q.Client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(q.URL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     wait,
	})
	if err != nil || len(out.Messages) == 0 {
		return nil, err
	}
	m := out.Messages[0]
	var s hosted.Shard
	if err := json.Unmarshal([]byte(aws.ToString(m.Body)), &s); err != nil {
		return nil, fmt.Errorf("decode shard message: %w", err)
	}
	return &hosted.Delivery{Shard: s, Handle: aws.ToString(m.ReceiptHandle)}, nil
}

func (q *Queue) Delete(ctx context.Context, d *hosted.Delivery) error {
	_, err := q.Client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(q.URL), ReceiptHandle: aws.String(d.Handle)})
	return err
}

// -------------------------------------------------------------------- S3

// S3API is the subset of the S3 client the blob store uses.
type S3API interface {
	PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// Blobs is a hosted.Blobs on S3. Traces stay private; URL returns a
// presigned link that expires.
type Blobs struct {
	Client  S3API
	Presign func(ctx context.Context, bucket, key string, ttl time.Duration) (string, error)
	Bucket  string
	Prefix  string        // optional key prefix
	TTL     time.Duration // presigned URL lifetime; default 7 days (the maximum)
}

// NewBlobs builds Blobs from an S3 client.
func NewBlobs(client *s3.Client, bucket string) *Blobs {
	pc := s3.NewPresignClient(client)
	return &Blobs{
		Client: client,
		Bucket: bucket,
		Presign: func(ctx context.Context, bucket, key string, ttl time.Duration) (string, error) {
			req, err := pc.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}, s3.WithPresignExpires(ttl))
			if err != nil {
				return "", err
			}
			return req.URL, nil
		},
	}
}

func (b *Blobs) key(k string) string { return b.Prefix + k }

func (b *Blobs) Put(ctx context.Context, key string, body []byte, contentType string) error {
	_, err := b.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(b.Bucket),
		Key:         aws.String(b.key(key)),
		Body:        bytes.NewReader(body),
		ContentType: aws.String(contentType),
	})
	return err
}

func (b *Blobs) URL(ctx context.Context, key string) (string, error) {
	ttl := b.TTL
	if ttl == 0 {
		ttl = 7 * 24 * time.Hour
	}
	return b.Presign(ctx, b.Bucket, b.key(key), ttl)
}

// -------------------------------------------------------------- DynamoDB

// DynamoAPI is the subset of the DynamoDB client the result store uses.
type DynamoAPI interface {
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	Query(ctx context.Context, in *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

// Results is a hosted.Results on a single DynamoDB table.
type Results struct {
	Client DynamoAPI
	Table  string
	// Retention is how long items live before TTL deletes them. Default 30 days.
	Retention time.Duration
	now       func() time.Time
}

func jobPK(id string) string { return "JOB#" + id }

// Keys used for items. Exported for tests and documentation.
func ShardSK(index int) string { return fmt.Sprintf("SHARD#%06d", index) }
func FailureSK(f hosted.Failure) string {
	return fmt.Sprintf("FAIL#%s#%020d", f.Group, f.Seed)
}

func (r *Results) expires() string {
	ret := r.Retention
	if ret == 0 {
		ret = 30 * 24 * time.Hour
	}
	now := time.Now
	if r.now != nil {
		now = r.now
	}
	return fmt.Sprint(now().Add(ret).Unix())
}

func (r *Results) put(ctx context.Context, pk, sk string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = r.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(r.Table),
		Item: map[string]ddbtypes.AttributeValue{
			"pk":      &ddbtypes.AttributeValueMemberS{Value: pk},
			"sk":      &ddbtypes.AttributeValueMemberS{Value: sk},
			"data":    &ddbtypes.AttributeValueMemberS{Value: string(b)},
			"expires": &ddbtypes.AttributeValueMemberN{Value: r.expires()},
		},
	})
	return err
}

// query returns the "data" of every item under pk whose sk starts with prefix.
func (r *Results) query(ctx context.Context, pk, prefix string) ([]string, error) {
	var out []string
	var start map[string]ddbtypes.AttributeValue
	for {
		res, err := r.Client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(r.Table),
			KeyConditionExpression: aws.String("pk = :pk AND begins_with(sk, :p)"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":pk": &ddbtypes.AttributeValueMemberS{Value: pk},
				":p":  &ddbtypes.AttributeValueMemberS{Value: prefix},
			},
			ExclusiveStartKey: start,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range res.Items {
			if d, ok := item["data"].(*ddbtypes.AttributeValueMemberS); ok {
				out = append(out, d.Value)
			}
		}
		if len(res.LastEvaluatedKey) == 0 {
			return out, nil
		}
		start = res.LastEvaluatedKey
	}
}

func (r *Results) PutJob(ctx context.Context, j hosted.Job) error {
	return r.put(ctx, jobPK(j.ID), "META", j)
}

func (r *Results) GetJob(ctx context.Context, id string) (hosted.Job, error) {
	if id == "" || strings.ContainsAny(id, "#") {
		return hosted.Job{}, hosted.ErrNotFound
	}
	res, err := r.Client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(r.Table),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: jobPK(id)},
			"sk": &ddbtypes.AttributeValueMemberS{Value: "META"},
		},
	})
	if err != nil {
		return hosted.Job{}, err
	}
	d, ok := res.Item["data"].(*ddbtypes.AttributeValueMemberS)
	if !ok {
		return hosted.Job{}, hosted.ErrNotFound
	}
	var j hosted.Job
	return j, json.Unmarshal([]byte(d.Value), &j)
}

func (r *Results) PutShardResult(ctx context.Context, s hosted.ShardResult) error {
	return r.put(ctx, jobPK(s.JobID), ShardSK(s.Index), s)
}

func (r *Results) ShardResults(ctx context.Context, jobID string) ([]hosted.ShardResult, error) {
	items, err := r.query(ctx, jobPK(jobID), "SHARD#")
	if err != nil {
		return nil, err
	}
	out := make([]hosted.ShardResult, 0, len(items))
	for _, d := range items {
		var s hosted.ShardResult
		if err := json.Unmarshal([]byte(d), &s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

func (r *Results) PutFailure(ctx context.Context, f hosted.Failure) error {
	return r.put(ctx, jobPK(f.JobID), FailureSK(f), f)
}

func (r *Results) Failures(ctx context.Context, jobID string) ([]hosted.Failure, error) {
	items, err := r.query(ctx, jobPK(jobID), "FAIL#")
	if err != nil {
		return nil, err
	}
	out := make([]hosted.Failure, 0, len(items))
	for _, d := range items {
		var f hosted.Failure
		if err := json.Unmarshal([]byte(d), &f); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	// Items are ordered by group then seed; Status expects seed order.
	sortFailures(out)
	return out, nil
}

func sortFailures(fs []hosted.Failure) {
	sort.Slice(fs, func(i, j int) bool { return fs[i].Seed < fs[j].Seed })
}

var (
	_ hosted.Queue   = (*Queue)(nil)
	_ hosted.Blobs   = (*Blobs)(nil)
	_ hosted.Results = (*Results)(nil)
)
