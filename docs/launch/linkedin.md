# LinkedIn post (draft)

I spent my evenings building something I have wanted for years as an
on-call engineer: a way to make distributed-systems bugs reproducible.

SimGo is an open-source deterministic simulation tester for Go. It runs a
whole cluster inside `go test`, controls the clock, network and disks from
a single seed, and throws crashes, partitions, process pauses and lost
packets at the code, thousands of runs a minute. Every failure comes with
a seed that replays it exactly, and a time-travel debugger you can scrub
backwards to see what each node did and believed.

What it found while I built the examples:

🔹 The real etcd raft library, with fsync batched on a timer (a common
"optimisation" that breaks raft's storage contract): two leaders in the
same term, and different data committed at the same log index.
🔹 A payment job runner that charged customers twice, from two different
bugs I did not know were there.
🔹 Lost acknowledged writes in a primary/backup service written with plain
net/http.

Things I learned along the way:

▪️ Determinism is a property you test, not assume. The check caught
etcd raft's crypto/rand election timeouts, and four bugs in my own
simulator. When a run does not reproduce, diff the traces before blaming
the code under test.
▪️ Go 1.25's testing/synctest makes simulating real goroutine code
possible, with one honest limit: two goroutines of one process racing at
the same instant cannot be replayed exactly. SimGo measures and reports it
instead of hiding it.
▪️ Always pair a safety test with a progress test. A system that does
nothing is perfectly safe.

It also has a hosted mode on AWS (Lambda, SQS, Fargate Spot, Terraform)
that shards millions of seeds and groups failures into distinct bugs.

Code, design docs and the experiments: github.com/Garvit-Jethwani/simgo

#golang #distributedsystems #testing #opensource #sre
