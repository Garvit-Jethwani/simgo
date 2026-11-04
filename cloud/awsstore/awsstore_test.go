package awsstore

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Garvit-Jethwani/simgo/cloud/hosted"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// fakeSQS keeps messages in memory.
type fakeSQS struct {
	mu   sync.Mutex
	msgs map[string]string
	next int
}

func (f *fakeSQS) SendMessage(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.next++
	f.msgs[fmt.Sprint(f.next)] = aws.ToString(in.MessageBody)
	return &sqs.SendMessageOutput{}, nil
}

func (f *fakeSQS) ReceiveMessage(_ context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if in.WaitTimeSeconds != 20 || in.MaxNumberOfMessages != 1 {
		return nil, fmt.Errorf("unexpected receive params: wait=%d max=%d", in.WaitTimeSeconds, in.MaxNumberOfMessages)
	}
	keys := make([]string, 0, len(f.msgs))
	for k := range f.msgs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		return &sqs.ReceiveMessageOutput{}, nil
	}
	return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{{Body: aws.String(f.msgs[keys[0]]), ReceiptHandle: aws.String(keys[0])}}}, nil
}

func (f *fakeSQS) DeleteMessage(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.msgs, aws.ToString(in.ReceiptHandle))
	return &sqs.DeleteMessageOutput{}, nil
}

func TestQueueRoundTrip(t *testing.T) {
	fake := &fakeSQS{msgs: map[string]string{}}
	q := &Queue{Client: fake, URL: "https://sqs/q"}
	ctx := context.Background()
	j := hosted.Job{ID: "j1", Repo: "https://x/y", Test: "TestX", Seeds: 10, ShardSize: 10}
	if err := q.Send(ctx, j.Shards()[0]); err != nil {
		t.Fatal(err)
	}
	d, err := q.Receive(ctx)
	if err != nil || d == nil || d.Shard.Job.ID != "j1" || d.Shard.Seeds != 10 {
		t.Fatalf("receive: %+v, %v", d, err)
	}
	if err := q.Delete(ctx, d); err != nil || len(fake.msgs) != 0 {
		t.Fatalf("delete: %v, %d left", err, len(fake.msgs))
	}
	if d, err := q.Receive(ctx); d != nil || err != nil {
		t.Fatalf("empty queue: %+v, %v", d, err)
	}
}

type fakeS3 struct{ objects map[string]string }

func (f *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	b, _ := io.ReadAll(in.Body)
	f.objects[aws.ToString(in.Bucket)+"/"+aws.ToString(in.Key)+" "+aws.ToString(in.ContentType)] = string(b)
	return &s3.PutObjectOutput{}, nil
}

func TestBlobsPutAndPresign(t *testing.T) {
	fake := &fakeS3{objects: map[string]string{}}
	var gotTTL time.Duration
	b := &Blobs{Client: fake, Bucket: "bkt", Prefix: "simgo/", Presign: func(_ context.Context, bucket, key string, ttl time.Duration) (string, error) {
		gotTTL = ttl
		return "https://" + bucket + ".s3/" + key + "?sig", nil
	}}
	if err := b.Put(context.Background(), "traces/j/seed-1.html", []byte("<html>"), "text/html"); err != nil {
		t.Fatal(err)
	}
	if fake.objects["bkt/simgo/traces/j/seed-1.html text/html"] != "<html>" {
		t.Fatalf("object not stored: %v", fake.objects)
	}
	u, err := b.URL(context.Background(), "traces/j/seed-1.html")
	if err != nil || u != "https://bkt.s3/simgo/traces/j/seed-1.html?sig" || gotTTL != 12*time.Hour {
		t.Fatalf("url: %q ttl=%v err=%v", u, gotTTL, err)
	}
}

// fakeDynamo stores items by pk and sk, supports the begins_with queries the
// store uses, and pages results like the real service.
type fakeDynamo struct {
	mu       sync.Mutex
	items    map[string]map[string]map[string]ddbtypes.AttributeValue
	pageSize int
}

func str(av ddbtypes.AttributeValue) string { return av.(*ddbtypes.AttributeValueMemberS).Value }

func (f *fakeDynamo) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	pk, sk := str(in.Item["pk"]), str(in.Item["sk"])
	if _, ok := in.Item["expires"].(*ddbtypes.AttributeValueMemberN); !ok {
		return nil, fmt.Errorf("item without numeric expires attribute")
	}
	if f.items[pk] == nil {
		f.items[pk] = map[string]map[string]ddbtypes.AttributeValue{}
	}
	f.items[pk][sk] = in.Item
	return &dynamodb.PutItemOutput{}, nil
}

func (f *fakeDynamo) GetItem(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &dynamodb.GetItemOutput{Item: f.items[str(in.Key["pk"])][str(in.Key["sk"])]}, nil
}

func (f *fakeDynamo) Query(_ context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if aws.ToString(in.KeyConditionExpression) != "pk = :pk AND begins_with(sk, :p)" {
		return nil, fmt.Errorf("unsupported key condition %q", aws.ToString(in.KeyConditionExpression))
	}
	pk, prefix := str(in.ExpressionAttributeValues[":pk"]), str(in.ExpressionAttributeValues[":p"])
	var sks []string
	for sk := range f.items[pk] {
		if strings.HasPrefix(sk, prefix) {
			sks = append(sks, sk)
		}
	}
	sort.Strings(sks)
	start := 0
	if in.ExclusiveStartKey != nil {
		after := str(in.ExclusiveStartKey["sk"])
		start = sort.SearchStrings(sks, after) + 1
	}
	out := &dynamodb.QueryOutput{}
	end := start + f.pageSize
	if end > len(sks) {
		end = len(sks)
	}
	for _, sk := range sks[start:end] {
		out.Items = append(out.Items, f.items[pk][sk])
	}
	if end < len(sks) {
		out.LastEvaluatedKey = map[string]ddbtypes.AttributeValue{"pk": &ddbtypes.AttributeValueMemberS{Value: pk}, "sk": &ddbtypes.AttributeValueMemberS{Value: sks[end-1]}}
	}
	return out, nil
}

func TestResultsWithServiceStatus(t *testing.T) {
	fake := &fakeDynamo{items: map[string]map[string]map[string]ddbtypes.AttributeValue{}, pageSize: 3}
	res := &Results{Client: fake, Table: "simgo"}
	svc := &hosted.Service{Queue: hosted.NewMemoryQueue(), Results: res, Blobs: hosted.DirBlobs{Dir: t.TempDir()}}
	ctx := context.Background()

	job, err := svc.Submit(ctx, hosted.Job{Repo: "https://github.com/x/y", Test: "TestX", Seeds: 5000, ShardSize: 1000})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		res.PutShardResult(ctx, hosted.ShardResult{JobID: job.ID, Index: i, Status: "done", SeedsRun: 1000})
	}
	// 10 failures of one bug and 1 of another, across several query pages.
	for seed := uint64(1); seed <= 10; seed++ {
		msg := fmt.Sprintf("job %d charged twice", seed*7)
		res.PutFailure(ctx, hosted.Failure{JobID: job.ID, Group: hosted.GroupKey("no double charge", msg), Seed: seed * 100, Invariant: "no double charge", Message: msg})
	}
	res.PutFailure(ctx, hosted.Failure{JobID: job.ID, Group: hosted.GroupKey("other", "x"), Seed: 50, Invariant: "other", Message: "x"})

	got, err := res.GetJob(ctx, job.ID)
	if err != nil || got.Seeds != 5000 {
		t.Fatalf("GetJob: %+v, %v", got, err)
	}
	if _, err := res.GetJob(ctx, "missing"); err != hosted.ErrNotFound {
		t.Fatalf("missing job: %v", err)
	}
	st, err := svc.Status(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if st.State != "done" || st.SeedsRun != 5000 || st.Failures != 11 || len(st.Groups) != 2 {
		t.Fatalf("status: state=%s seeds=%d failures=%d groups=%d", st.State, st.SeedsRun, st.Failures, len(st.Groups))
	}
	if g := st.Groups[0]; g.Count != 10 || g.Seeds[0] != 100 || g.Message != "job 7 charged twice" {
		t.Fatalf("largest group: %+v", g)
	}
}
