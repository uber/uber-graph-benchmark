Uber Graph Benchmark (UGB)
====================================

Slides: [Uber Graph Benchmark Framwork](./UberGraphBenchmarkFramework.pdf)

Getting Started
---------------

1. Check out the repo

2. Set up a database to benchmark. There is a README file under each binding 
   directory. List out all modules by
   ```sh
   ./gradlew projects
   ```

3. Run benchmark on db

  ```sh
  # generates and writes to redis db, then reads with subgraph queries
  ./gradlew execute -PmainArgs="-db com.uber.ugb.db.redis.RedisDB -w -g benchdata/graphs/trips -b benchdata/workloads/workloada -r"

  # generates and writes to redis db, then reads with subgraph queries
  ./gradlew execute -PmainArgs="-db com.uber.ugb.db.cassandra.CassandraDB -w -g benchdata/graphs/trips -b benchdata/workloads/workloada -r"

  # this generate vertices and edges and write to noop, used for measuring data gen performance
  ./gradlew execute -PmainArgs="-db com.uber.ugb.db.NoopDB -g benchdata/graphs/trips -b benchdata/workloads/workloada -w"

  # this generate vertices and edges and write as csv to System.out or a file
  ./gradlew execute -PmainArgs="-db com.uber.ugb.db.CsvOutputDB -g benchdata/graphs/trips -b benchdata/workloads/workloada -w"

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



Build
---------------
  * create jar
   ```sh
   ./gradlew jar
   ```
  * build fat jar for spark
   ```sh
   ./gradlew build
   ```


Run on Spark
---------------
Here is one example on how to run spark
```
#!/usr/bin/env bash

cd ugsb

YARN_CONF_DIR=/etc/hadoop/conf /home/spark-2.1.0/bin/spark-submit \
--class "com.uber.ugb.Benchmark" \
--master yarn \
--deploy-mode client \
--driver-memory 6G \
--executor-memory 6G \
--executor-cores 2 \
--driver-cores 2 \
--num-executors 10 \
--conf spark.yarn.executor.memoryOverhead=2048 \
--driver-class-path '/etc/hive/conf' \
build/libs/ugb-all-0.0.15.jar \
"-db com.uber.ugb.db.cassandra.CassandraDB -w -g benchdata/graphs/trips -b benchdata/workloads/workloada -r -s"

echo $?

```
