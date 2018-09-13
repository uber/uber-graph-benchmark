Uber Graph Benchmark (UGB)
====================================

Getting Started
---------------

1. Check out the repo

2. Set up a database to benchmark. There is a README file under each binding 
   directory. List out all modules by
   ```sh
   ./gradlew projects
   ```

3. Run benchmark on db

  Here is an example of running RedisDB
  ```sh
  @ generates and writes to redis db, then reads with subgraph queries
  ./gradlew execute -PmainArgs="-db com.uber.ugb.db.redis.RedisDB -w -g benchdata/graphs/trips -b benchdata/workloads/workloada -r"

  # this generate vertices and edges and write to noop, used for measuring data gen performance
  ./gradlew execute -PmainArgs="-db com.uber.ugb.db.NoopDB -g benchdata/graphs/trips -b benchdata/workloads/workloada -w"

  ```

Customization
---------------


Set environment variables in

  ```text
  benchdata/workloads/env.properties
  ```

To add a new DB implementation, consider inherit from
  * com.uber.ugb.db.KeyValueDB
    
    This stores the adjacency list in one blob.

  * com.uber.ugb.db.PrefixKeyValueDB
  
    This stores the adjacency list with the same prefix. The edge writes could be faster than KeyValueDB.

  * com.uber.ugb.db.GremlinDB

    This processes gremlin queries directly.

