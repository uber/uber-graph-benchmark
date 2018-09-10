Uber Graph Service Benchmark (UGSB)
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
  ./gradlew execute -PmainArgs="-db com.uber.ugsb.db.redis.RedisDB -w -g benchdata/graphs/trips -b benchdata/workloads/workloada -r"
  ```
