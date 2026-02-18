# Spark Concepts

## Spark Configuration Precedance Order

- InitialConfigs (inherited from spark-submit) = cli-options + spark-defaults.conf + env_vars [merged config]
- Session Configs Application-Code (SparkConf) + Initial Configs [merged config]
- Precedence Order of config merging: ApplicationCode > IntialConfig(CLI-options > spark-defaults.conf > env_vars)

## Spark Partitions and Transformations
- Spark Dataframes are broken into partition (HDFS), each partition (on diff nodes) are in-memory logically (feels like on single node) for reading/writing.

- Spark Dataframe are stored as the distributed partitions of DataFrames

### Transformation based on dependency types
- Narrow Dependency Transformation: A transformation performed independently on a single partition to produce valid results.

    e.g, where clause transformation

    - Partition can independently filter records without shuffle/sort exchange.
    - Partitions are combined to get final records.

- Wide Dependency Transformation: A transformation that requires data from other partitions to produce valid results.

    e.g, groupBy, count transformation

    - Partition are combined & sorted.
    - Then repartitioned group-wise and aggreagate function are applied on each partition individually.
    - Partitions are combined to get the result.

### Spark Actions
1. Read
2. Write
3. Collect
4. Show

### Spark Transformation
1. Spark SQL/Pandas APIs for transformations
2. e.g, where, select, groupBy, count