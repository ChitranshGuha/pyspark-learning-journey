from pyspark.sql.functions import sum

# Data and Schemas provided for you
customers_data = [(1, "Alice", "NY"), (2, "Bob", "CA"), (3, "Charlie", "NY")]
customers_schema = ["CustomerID", "Name", "State"] # PySpark can accept simple lists for schemas if types aren't critical

purchases_data = [(1, 150), (1, 200), (2, 50), (3, 500), (3, 100)]
purchases_schema = ["CustomerID", "Amount"]

# Assume spark.createDataFrame is already run, giving you:
# customers_df and purchases_df
customers_df = spark.createDataFrame(data=customers_data, schema=customers_schema)
purchases_df = spark.createDataFrame(data=purchases_data, schema=purchases_schema)
joined_df = purchases_df.join(customers_df,on="CustomerID",how="inner")
# spend_df = joined_df.groupBy("Name").sum("Amount").alias("Total_Spent")
# joined_df.groupBy("Name").sum("Amount").withColumnRenamed("sum(Amount)", "Total_Spent") --correct of above
# Wrap the sum function inside .agg() so you can apply the alias to the column
spend_df = joined_df.groupBy("Name").agg(sum("Amount").alias("Total_Spent"))
spend_df.show()
def return_df():
    return spend_df