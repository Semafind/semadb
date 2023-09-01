To restrict CPU to 0 and 1 run:

```bash
taskset -c 0,1 go run ./
```

Some tools and repos that might be useful:

- https://github.com/patrickmn/go-cache or https://github.com/jellydator/ttlcache for caching
- https://github.com/ddosify/ddosify for load testing
- https://github.com/tsenart/vegeta for load testing
- https://github.com/go-swagger/go-swagger for API creation
- https://github.com/go-resty/resty for HTTP REST client
- https://github.com/uber-go/zap for logging
- https://github.com/rs/zerolog also for logging
- https://github.com/cdipaolo/goml for clustering
- https://github.com/stretchr/testify for testing assertions
- https://github.com/gin-gonic/gin for web framework
- https://github.com/labstack/echo also for web framework
- https://github.com/cespare/xxhash for fast hashing
- https://github.com/dustin/go-humanize for human readable logging
- https://github.com/ko-build/ko for building
