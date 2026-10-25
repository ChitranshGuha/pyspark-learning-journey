from task2 import return_df

df = return_df() #returns spend_df from prev py file

df.write.format("delta").mode("append").saveAsTable("premium_customers")

restored_df = spark.read.format("delta").option("versionAsOf",2).table("premium_customers")

restored_df.show()
