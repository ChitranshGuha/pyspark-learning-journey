from pyspark.sql.types import StructType, StructField, StringType, IntegerType,BooleanType
from pyspark.sql.functions import col,lit


schema = StructType([
    StructField("Username",StringType(),True),
    StructField("Age",IntegerType(),True),
    StructField("isActive",BooleanType(),True)    
])

data = [("John", 25, True), ("Sara", 17, True), ("Mike", 30, False)]

users_df = spark.createDataFrame(data=data, schema=schema)

df1 = users_df.filter(col("Age")>=18 & col("isActive")==True)
df1 = users_df.filter((col("Age") >= 18) & (col("isActive") == True))
df2 = df1.withColumn("Status",lit("Premium"))

df2.show()