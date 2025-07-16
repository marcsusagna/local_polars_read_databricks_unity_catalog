# Description
How to run polars locally (linux) for EDA on data from Databricks Unity Catalog

# How does this work

## Prerequisites

### On your local machine

1. Have a python version installed (see .python-version)
2. If this is the first time, run the commands in devops/bootstrap.sh. If you have already installed the dependences, run the commands in devops/start_environment.sh
3. Create a file called devops/.env filling the values left as placeholder in devops/.env.example
4. Open jupyter notebook example.ipynb, fill in with the table you want to read and the columns you want to agg with

Note that the example notebook shows that you can prune columns as well as do predicate pushdown

### On databricks UC 

This solution is based credential vending, so see [Credential Vending](https://docs.azure.cn/en-us/databricks/external-access/credential-vending#requirements) to understand the prerequisites. 

## Components used

### Arrow

Spark 4 has made a major effort to improve interaction with PyArrow better. See here [Spark 4 Arrow](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas).html as well as this method [toArrow](https://spark.apache.org/docs/latest//api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.toArrow.html) (claimed to be released in Spark 4, even though late spark 3.5 version also include it). 

In parallel, Arrow is gaining popularity to read parquet files from disk directly into memory for any other data processing framework, like Polars or Duckdb. 

### delta-rs

See [delta-rs](https://github.com/delta-io/delta-rs). This library is facilitating the interaction with a delta lake (Unity Catalog or any other solution for it). It abstracts the complexity to connect, for instance, to Unity Catalog, by leveraging the UC API. 

In particular, in this repository we use pyarrow to bring a delta table into arrow and then load it into polars. Similar can be done for duckdb

# Alternatives to the proposed setup

## Read UC objects directly from polars

Polars is looking into this directly ([Polars UC](https://docs.pola.rs/api/python/stable/reference/catalog/api/polars.Catalog.html#polars.Catalog)) so we don't need to use deltalake library. However, it is marked unstable as of July 2025 and you would be locking yourself into polars, when a local client could be using other tools like duckdb, pandas or daft. I recommend reading this [Delta without spark](https://delta.io/blog/delta-lake-without-spark/) to understand this flexibility .

## Bypass UC and access cloud storage directly

Accessing locally data stored in Databricks' Catalog has always been possible. In fact, that has been from day 1 Databricks strategy. Not to be a closed platform. 

However, accessing directly storage, apart from the complexity it entails to deal with each cloud provider differently, has the problem that you need to bypass the governance layer of UC. It is very powerful that now we can, through UC, get to the relevant parquet files of a delta table, as it keeps data governance on the lakehouse centralized (i.e. audit control as well as single place for data access policies)


## Limitations

Polars has demonstrated to be very fast for small/medium data compared to spark. Moreover, Polars allows to execute in eager or in lazy mode (optimizations). Lazy mode helps performing predicate pushdown and column pruning, which is critical to reduce disk I/O. The issue with this repository is that because we use deltalake library and pyarrow to read the delta table, polars is unable to pass down these optimizations and it is required for the user to specify that smartly when reading the data. 

Probably the development of https://docs.pola.rs/api/python/stable/reference/catalog/api/polars.Catalog.html#polars.Catalog will lead to having that integrated, but at the loss of not having an abstraction for any processing engine like deltalake library does. 

As a consequence, spark might be faster than polars, if the user is not able to specify the partition columns and predicate pushdown, since spark resolves this automatically during the query plan. 

# Value proposition: Why run this locally / out of Databricks?

## 1. Not all data use cases need to process lots of data

The lakehouse has become the central platform from which data is taken for analytical purposes. However, not every data user needs to access massive amounts of data. 

In Databricks, any cluster (serverless or not) comes with Spark, which generates a significant overhead that translates in either:
- Inefficient use of cluster memory or 
- Slow performance in spark queries. 

Both these consequences have a final impact on your cloud compute costs bill.

### Inefficient use of cluster memory

This is particularly important in Single Node clusters, where driver and executor processes need to be handled. For instance in Azure Databricks, if you take a Standard_D4ds_v5 VM (16GB RAM, 4 Cores) this is how the memory is distributed:

- OS & linux processes ~ 2 GB. Reamining: 16 - 2= 14GB. 
- Heap memory for executor (JVM Heap Space): This is defined based on the VM type chosen. These limits are enforced by Databricks based on heuristics. In particular: (memory_size\*0.97 - 4800 MB)\* 0.8 (https://kb.databricks.com/en_US/clusters/spark-shows-less-memory). This relates vaguely to the setting spark.executor.memory, which you can't increase when defining your cluster in databricks (see that for this specific VM type, it is capped to 8874 MB)
- So our executor process (JVM Heap Space) has roughly (14000\*0.97 - 4800)\*0.8 = 7024 MB
- From here we need to define how much is available for Execution and Storage Memory (M) (see [Memory Management](https://spark.apache.org/docs/latest/tuning.html#memory-management-overview)). The amount of memory for these is: (JVM Heap Space - 300MB)\*spark.memory.fraction. Let's take the 7024 MB as JVM Heap Space for default settings: then M = (7024 - 300) \* 0.6 = 4034 MB. 
- From this memory the execution memory can take at most M*spark.memory.storageFraction, so in our case for default settings: 4034\*0.5=2017

As we can see, from 16GB of RAM in a VM we ended up using only 2 GB at most for execution (joins, groupby, etc...) as it can't take the 2GB reserved for caching (spark.memory.storageFraction). Similarly we can't store in memory more than this threshold as well, since M is limited to 4034. Note the numbers above are to give a rough idea, we are using GB instead of GiB (this is how spark configuration is defined) and we don't have the exact numbers from Databricks heuristics. However, you can see this estimated numbers directly from your spark UI > executors > Storage Memory. 

Long story short: Spark was defined for massive amounts of data, using it for small amount of data leads to inefficient resource usage, which in cloud terms, means more costs. What could we do with 16GB RAM in an engine optimized for "small" data processing (polars, duckdb...)?

### Slow performance in spark queries (small data!)

Data Scientists often complain about how slow Spark is. That's because Spark is not the best at in-memory data processing. While one can tweak it playing with caching, it is not as straightforward as other engines. Don't get me wrong, Spark is great at massive amount of data processing (think of it like bronze, silver and even gold transformations for big Data Marts), but not for ALL data processing use cases.

This time lost due to spark is uptime cost of machines we need to pay for. 

## 2. Data is unknown, there is lot of exploration thrown away

Data users need to usually explore the data before getting to any conclusion on what needs to be implemented. As claimed by many experts, most of a Data Scientist job is "thrown away", because it is only EDA before getting into thinking a final statistics test or ML model. I would claim that same happens to Data Engineers and Data Analyst when building the gold layer / data model for a specific report. Data edge cases keep happening and you need to adapt for it. 

In this case, running for instance polars, duckdb or even spark locally can provide an engine to run this EDA without incurring costs within Databricks. 

Once a clear pipeline or model needs to be implemented, it is always better to run that within Databricks, as many benefits come for it: Logging, observability on cluster / app, lineage (if spark), collaboration and sharing...

