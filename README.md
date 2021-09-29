# CS744-Assignment1

This [Github repository](https://github.com/i-am-rahul/CS744-Assignment1/tree/master) houses Spark applications for UW Madison CS 744 [Assignment 1](https://pages.cs.wisc.edu/~shivaram/cs744-fa21/assignment1.html). The language of choice is Scala and it was picked over Python or Java since Spark provides a nice interactive Scala shell for quick prototyping. The members of the group are:
| Member          | Email         | 
| ----------------|:-------------:| 
| Rahul Choudhary | rahul.choudhary@wisc.edu | 
| Abhinav Agarwal | agarwal72@cs.wisc.edu      | 
| Kaustubh Khare  | kkhare@wisc.edu      | 

The assignment has two parts to it:
1) A sorting application
2) A PageRank application

We implement the two tasks in the assignment in Scala using [Dataset](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html) and Dataframe (alias Dataset[Row]) to be the immutable distributed collections of data. The following must be installed in order to be able to run our code:
1) Spark
2) Hadoop

In addition to the task themselves, we create a few utility scripts:
1) **run.sh**: This script is used to submit the scala application class to Spark. The parameters the script expects are as follows:
```shell
./run.sh <master_ip> <jar_path> <run_class> <input_hdfs_location> <output_hdfs_location> <persists> <col_name_to_partition> <number_partition>
```

```shell
example: ./run.sh c220g2-010607vm-1.wisc.cloudlab.us pagerank_2.12-0.1.jar org.rakab.SparkPageRank "hdfs://10.10.1.1:9000/data/task2/enwiki-pages-articles/" "hdfs://10.10.1.1:9000/data/task2/output/" "true" "node" 100 
```

2) **start_stats_collection.sh**: This script is used to collect system level statistics across all three cluster nodes. It internally spawns a shell script called collect_updated_stats.sh on each node which monitors memory usage, network, disk and CPU statistics. These statistics are written to a folder called  All you need to do is run **start_stats_collection.sh** only on the master after **run.sh** is used to start the Spark application. The parameters the script expects are as follows:
```shell
./start_stats_collection.sh <app_config_descriptor> <stats_poll_interval_in_seconds>
```

```shell
example: ./start_stats_collection.sh "persist_partition_200_75p_fail" 5
```
The script creates the following files: 
* $(pwd)/stats_updated/<app_config_descriptor>/cpu
* $(pwd)/stats_updated/<app_config_descriptor>/disk
* $(pwd)/stats_updated/<app_config_descriptor>/memory
* $(pwd)/stats_updated/<app_config_descriptor>/network

3) **stop_stats_collection.sh**: This script is used to stop the system level statistics collection once the Spark job is complete. Simply call the following:
```shell
./stop_stats_collection.sh
```
on the master, once the application completes. This script would stop stats collection on all three nodes.

Finally, an example end to end invocation would look like this:

```shell
rahul_@node0:~$ nohup ./run.sh c220g2-010607vm-1.wisc.cloudlab.us pagerank_2.12-0.1.jar org.rakab.SparkPageRank "hdfs://10.10.1.1:9000/data/task2/enwiki-pages-articles/" "hdfs://10.10.1.1:9000/data/task2/output/" "true" "node" 100 &
rahul_@node0:~$ ./start_stats_collection.sh "persist_partition_100" 5

......application_completes......

rahul_@node0:~$ ./stop_stats_collection.sh
```

Now we look at the two main tasks of the assignment.

## Sort (Part 2)

In order to run the Sorting application, we must first create the project jar and submit it to `run.sh`. The steps are:
1. Clone this repository.
2. Package it using [sbt](https://alvinalexander.com/scala/sbt-how-to-compile-run-package-scala-project/).
3. Copy the jar file generated to your home directory.
4. Submit the jar file and the app class name(org.rakab.Sorting) to run.sh along with the input and output file paths.
5. Enjoy.
```shell
nohup ./run.sh <clusterHeadNodeUrl> pagerank_2.12-0.1.jar org.rakab.Sorting <inputHdfsPath> <outputHdfsPath> - - -1 &
Example:
nohup ./run.sh c220g2-010607vm-1.wisc.cloudlab.us pagerank_2.12-0.1.jar org.rakab.Sorting "hdfs://10.10.1.1:9000/data/task1/input/" "hdfs://10.10.1.1:9000/data/task1/output/" - - -1 &
```

## PageRank (Part 3)

In order to run the PageRank application, we must first create the project jar and submit it to `run.sh`. The steps are:
1. Clone this repository.
2. Package it using [sbt](https://alvinalexander.com/scala/sbt-how-to-compile-run-package-scala-project/).
3. Copy the jar file generated to your home directory.
4. Submit the jar file and the app class name(org.rakab.SparkPageRank) to run.sh along with the input and output file paths and different application parameters (namely whether to persist or not, which column in the links list Dataframe to partition by, and how many numbers of partition needed).
5. Enjoy.

The different configurations we report are:
#### No persistence and no partitioning
(For this case, we remove all explicit persistence and partitioning code from our SparkPageRank.scala file and generate a pagerank_vanilla_2.12-0.1.jar jar)
```shell
nohup ./run.sh <clusterHeadNodeUrl> pagerank_vanilla_2.12-0.1.jar org.rakab.SparkPageRank <inputHdfsPath> <outputHdfsPath> "false" "node" -1 &
Example:
nohup ./run.sh c220g2-010607vm-1.wisc.cloudlab.us pagerank_vanilla_2.12-0.1.jar org.rakab.SparkPageRank "hdfs://10.10.1.1:9000/data/task2/enwiki-pages-articles/" "hdfs://10.10.1.1:9000/data/task2/output2/" "false" "node" -1 &
```

#### No persistence but with partitioning on column 'node' of linksList dataframe
```shell
nohup ./run.sh <clusterHeadNodeUrl> pagerank_2.12-0.1.jar org.rakab.SparkPageRank <inputHdfsPath> <outputHdfsPath> "false" "node" -1 &
Example:
nohup ./run.sh c220g2-010607vm-1.wisc.cloudlab.us pagerank_2.12-0.1.jar org.rakab.SparkPageRank "hdfs://10.10.1.1:9000/data/task2/enwiki-pages-articles/" "hdfs://10.10.1.1:9000/data/task2/output2/" "false" "node" -1 &
```

#### No persistence but with partitioning of linksList dataframe into 200 partitions
```shell
nohup ./run.sh <clusterHeadNodeUrl> pagerank_2.12-0.1.jar org.rakab.SparkPageRank <inputHdfsPath> <outputHdfsPath> "false" "node" 200 &
Example:
nohup ./run.sh c220g2-010607vm-1.wisc.cloudlab.us pagerank_2.12-0.1.jar org.rakab.SparkPageRank "hdfs://10.10.1.1:9000/data/task2/enwiki-pages-articles/" "hdfs://10.10.1.1:9000/data/task2/output2/" "false" "node" 200 &
```

#### With persistence and partitioning of linksList dataframe into 100 partitions
```shell
nohup ./run.sh <clusterHeadNodeUrl> pagerank_2.12-0.1.jar org.rakab.SparkPageRank <inputHdfsPath> <outputHdfsPath> "true" "node" 100 &
Example:
nohup ./run.sh c220g2-010607vm-1.wisc.cloudlab.us pagerank_2.12-0.1.jar org.rakab.SparkPageRank "hdfs://10.10.1.1:9000/data/task2/enwiki-pages-articles/" "hdfs://10.10.1.1:9000/data/task2/output2/" "true" "node" 100 &
```

#### With persistence and partitioning of linksList dataframe into 200 partitions
```shell
nohup ./run.sh <clusterHeadNodeUrl> pagerank_2.12-0.1.jar org.rakab.SparkPageRank <inputHdfsPath> <outputHdfsPath> "true" "node" 200 &
Example:
nohup ./run.sh c220g2-010607vm-1.wisc.cloudlab.us pagerank_2.12-0.1.jar org.rakab.SparkPageRank "hdfs://10.10.1.1:9000/data/task2/enwiki-pages-articles/" "hdfs://10.10.1.1:9000/data/task2/output2/" "true" "node" 200 &
```

#### With persistence and partitioning of linksList dataframe into 200 partitions, and killing a worker at 25% progress of job
On node 0:
```shell
nohup ./run.sh <clusterHeadNodeUrl> pagerank_2.12-0.1.jar org.rakab.SparkPageRank <inputHdfsPath> <outputHdfsPath> "true" "node" 200 &
Example:
nohup ./run.sh c220g2-010607vm-1.wisc.cloudlab.us pagerank_2.12-0.1.jar org.rakab.SparkPageRank "hdfs://10.10.1.1:9000/data/task2/enwiki-pages-articles/" "hdfs://10.10.1.1:9000/data/task2/output2/" "true" "node" 200 &
```

On node 2 at 25% time elapsed: 
```shell
sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"
kill -9 <spark_worker_pid>
```

#### With persistence and partitioning of linksList dataframe into 200 partitions, and killing a worker at 75% progress of job
On node 0:
```shell
nohup ./run.sh <clusterHeadNodeUrl> pagerank_2.12-0.1.jar org.rakab.SparkPageRank <inputHdfsPath> <outputHdfsPath> "true" "node" 200 &
Example:
nohup ./run.sh c220g2-010607vm-1.wisc.cloudlab.us pagerank_2.12-0.1.jar org.rakab.SparkPageRank "hdfs://10.10.1.1:9000/data/task2/enwiki-pages-articles/" "hdfs://10.10.1.1:9000/data/task2/output2/" "true" "node" 200 &
```

On node 2 at 75% time elapsed: 
```shell
sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"
kill -9 <spark_worker_pid>
```
For the last two cases, we first run the job 5 times with all workers healthy and note the average completion time. We then use this completion time to determine 25% time elapsed and 75% time elapsed.

