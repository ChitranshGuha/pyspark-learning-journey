# Delta Lake is an open-source storage layer that sits on top of your raw cloud files. It saves data underneath as highly compressed Parquet files, but it adds a crucial transaction log. This gives your cloud storage superpowers:

# ACID Transactions: If a PySpark job fails halfway through writing, Delta cancels the whole thing. No partial, corrupt data.

# Schema Enforcement: If someone tries to write a string into an integer column, Delta blocks it.

# Time Travel: Delta tracks every change. You can query what a table looked like exactly 3 days ago.

# Writing a DataFrame to a Delta table
spend_df.write.format("delta").mode("overwrite").saveAsTable("customer_spend_summary")

# Reading it back
df = spark.read.format("delta").table("customer_spend_summary")

# Load version 0 (the very first time the table was created)
old_df = spark.read.format("delta").option("versionAsOf", 0).table("customer_spend_summary")