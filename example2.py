from pyspark.sql.functions import sum, avg

# Assuming we have a sales_df with 'Region' and 'Revenue'
summary_df = sales_df.groupBy("Region").agg(
    sum("Revenue").alias("Total_Revenue"),
    avg("Revenue").alias("Average_Revenue")
)

# df1.join(df2, on="matching_column_name", how="join_type")

# Example: Joining users to their purchases
enriched_df = purchases_df.join(users_df, on="Username", how="inner")