# To stop PySpark from repeating work, you use the .cache() command. This tells the Worker Nodes to keep the DataFrame in their RAM after the first action. Any subsequent actions will just read from the RAM instantly.

clean_df = spark.read.csv("huge_file.csv").filter(col("status") == "valid")

# Tell Spark to memorize this DataFrame
clean_df.cache()

# Spark reads the CSV and filters it, then saves the result in RAM
print(clean_df.count()) 

# Spark skips the read/filter steps entirely and just prints from RAM!
clean_df.show()

# ------------------------------------------------------------

# Your data is split into chunks called partitions. If you filter 100 million rows down to 100 rows, you might be left with 200 partitions that are almost entirely empty. This means 200 worker threads are spinning up just to process half a row each, causing massive overhead.

# You fix this using .coalesce(), which shrinks the number of partitions without triggering a heavy network Shuffle.

# Shrink the data down to 4 partitions before saving it
small_df = clean_df.coalesce(4)

# (Note: There is also a .repartition() command. You use that when you want to INCREASE partitions to use more worker cores, but it forces a heavy network Shuffle).