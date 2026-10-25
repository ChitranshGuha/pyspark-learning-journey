# PySpark & Databricks Learning Journey

This repository contains my learning progression and core conceptual examples for distributed data processing using Apache Spark (PySpark) and Databricks.

## Core Concepts Mastered

### 1. Cluster Architecture & Execution
* **Driver & Worker Nodes:** Understanding the delegation of tasks from the Driver (manager) to the Worker nodes (executors).
* **Lazy Evaluation:** How PySpark builds a Directed Acyclic Graph (DAG) of transformations but delays execution until an Action (`.show()`, `.count()`, `.write()`) is triggered.
* **Narrow vs. Wide Transformations:** Understanding the performance impact of network Shuffles caused by operations like `.groupBy()` and `.join()`.

### 2. DataFrame Operations (Transformations)
* **Explicit Schemas:** Defining exact data types using `StructType` and `StructField` to avoid expensive `inferSchema` operations.
* **Column Manipulation:** Using `col()`, `lit()`, and `.withColumn()` to filter data and engineer new features.
* **Handling Bitwise Operators:** Properly wrapping multiple conditions in parentheses `(col("Age") >= 18) & (col("isActive") == True)`.

### 3. Real-World Processing
* **Aggregations:** Grouping data and applying aggregate functions correctly using the `.agg()` wrapper to assign aliases (e.g., `.agg(sum("Amount").alias("Total_Spent"))`).
* **Joins:** Combining distributed tables using standard SQL-like join patterns (`inner`, `left`).

### 4. Databricks & Delta Lake
* **ACID Transactions & Storage:** Writing DataFrames to cloud storage using the `.format("delta")` specification with `mode("append")` or `mode("overwrite")`.
* **Time Travel:** Querying historical versions of a table for data recovery using `.option("versionAsOf", <version_number>)`.

### 5. Performance Tuning
* **Caching:** Using `.cache()` to store intermediate DataFrames in worker memory, preventing PySpark from re-evaluating the entire DAG for multiple actions.
* **Partition Management:** Using `.coalesce()` to reduce the number of partitions before writing small, highly-filtered DataFrames to storage, avoiding network shuffles and creating optimized file sizes.

---

## Code Reference: The Optimized Pipeline

Here is an example of a fully optimized PySpark script demonstrating reading, filtering, caching, and partitioning data:

```python
from pyspark.sql.functions import col

# 1. Read the data using the spark session from Delta storage
df = spark.read.format("delta").table("server_logs")

# 2. Filter for specific events (Narrow Transformation)
log_df = df.filter(col("log_level") == "ERROR")

# 3. Cache it in memory so the DAG isn't re-evaluated twice
log_df.cache()

# 4. Action 1: Count and print
error_count = log_df.count()
print(f"Total errors found: {error_count}")

# 5. Optimize partitions for the heavily filtered (smaller) dataset
compact_df = log_df.coalesce(2)

# 6. Action 2: Write the optimized data to a new Delta table
compact_df.write.format("delta").mode("overwrite").saveAsTable("error_log_summary")
```