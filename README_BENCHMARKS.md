# Benchmark Results

Expected benchmark output on a modern laptop (Intel Core i3-4010U @ 1.70GHz).

## Running Benchmarks

```bash
go test -bench=. -benchmem -count=3 ./...
```

## Expected Results

```
goos: linux
goarch: amd64
pkg: github.com/seanrobmerriam/rate-limiter-go
cpu: Intel(R) Core(TM) i3-4010U CPU @ 1.70GHz
BenchmarkTokenBucket_MemoryStore-4              	 4409268	       270.3 ns/op	       0 B/op	       0 allocs/op
BenchmarkTokenBucket_MemoryStore-4              	 4216912	       289.1 ns/op	       0 B/op	       0 allocs/op
BenchmarkTokenBucket_MemoryStore-4              	 4310850	       272.1 ns/op	       0 B/op	       0 allocs/op
BenchmarkSlidingWindow_MemoryStore-4            	  408703	      2803 ns/op	    2688 B/op	       1 allocs/op
BenchmarkSlidingWindow_MemoryStore-4            	  425482	      2808 ns/op	    2688 B/op	       1 allocs/op
BenchmarkSlidingWindow_MemoryStore-4            	  344436	      3941 ns/op	    2688 B/op	       1 allocs/op
BenchmarkTokenBucket_MemoryStore_Parallel-4     	 2324403	       502.4 ns/op	       0 B/op	       0 allocs/op
BenchmarkTokenBucket_MemoryStore_Parallel-4     	 2348304	       511.0 ns/op	       0 B/op	       0 allocs/op
BenchmarkTokenBucket_MemoryStore_Parallel-4     	 2335365	       499.7 ns/op	       0 B/op	       0 allocs/op
BenchmarkSlidingWindow_MemoryStore_Parallel-4   	  354394	      3379 ns/op	    2688 B/op	       1 allocs/op
BenchmarkSlidingWindow_MemoryStore_Parallel-4   	  353376	      3384 ns/op	    2688 B/op	       1 allocs/op
BenchmarkSlidingWindow_MemoryStore_Parallel-4   	  352011	      3441 ns/op	    2688 B/op	       1 allocs/op
BenchmarkMiddleware_Allowed-4                   	  265716	      4526 ns/op	    1588 B/op	      17 allocs/op
BenchmarkMiddleware_Allowed-4                   	  225134	      4788 ns/op	    1587 B/op	      17 allocs/op
BenchmarkMiddleware_Allowed-4                   	  267548	      4475 ns/op	    1588 B/op	      17 allocs/op
BenchmarkMiddleware_Denied-4                    	  259249	      4577 ns/op	    1615 B/op	      17 allocs/op
BenchmarkMiddleware_Denied-4                    	  262687	      4537 ns/op	    1615 B/op	      17 allocs/op
BenchmarkMiddleware_Denied-4                    	  220692	      4562 ns/op	    1615 B/op	      17 allocs/op
```

## Analysis

### Token Bucket (Memory Store)

- **Throughput**: ~4.3 million operations/second
- **Latency**: ~275 ns/op
- **Allocations**: 0 B/op, 0 allocs/op

The token bucket algorithm achieves excellent performance due to:
- O(1) complexity for checking and updating state
- Zero allocations per operation (uses existing map entries)
- Simple float arithmetic for token calculations

### Sliding Window (Memory Store)

- **Throughput**: ~390,000 operations/second
- **Latency**: ~2800 ns/op
- **Allocations**: 2688 B/op, 1 alloc/op

The sliding window is slower because:
- O(n) complexity where n = requests in current window
- Allocates a new slice on each check to filter expired timestamps
- More memory overhead per key

### Parallel Performance

- **TokenBucket Parallel**: ~2.3M ops/sec (~500 ns/op)
- **SlidingWindow Parallel**: ~350K ops/sec (~3400 ns/op)

Parallel performance shows the effect of mutex contention. Token bucket maintains better throughput due to its simpler state management.

### Middleware Overhead

- **Allowed requests**: ~4,500 ns/op, 1588 B, 17 allocs
- **Denied requests**: ~4,500 ns/op, 1615 B, 17 allocs

Middleware adds ~4μs overhead including:
- HTTP request handling
- JSON encoding for error responses
- Response header manipulation

## Performance Recommendations

1. **Choose Token Bucket** for most use cases - 10x faster with zero allocations
2. **Use Sliding Window** only when precise window boundaries are required
3. **Consider Redis store** for distributed systems - expect 10-100x slower depending on network latency
4. **Benchmark your specific workload** - real-world patterns may differ from these microbenchmarks
