from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# We define the blueprint of our table
my_schema = StructType([
    StructField("EmployeeName", StringType(), True), # True means the field can be empty (nullable)
    StructField("Department", StringType(), True),
    StructField("Salary", IntegerType(), True)
])
# You can let Spark guess the data types of your data (called inferSchema), but in data engineering, this is a bad practice. To guess, Spark has to read through your data, which wastes time and compute power. Instead, we explicitly define a Schema.


# --------------------------------------------------------------------
# --------------------------------------------------------------------
# Raw data in Python
data = [
    ("Alice", "Engineering", 85000),
    ("Bob", "Marketing", 70000),
    ("Charlie", "Engineering", 90000)
]

# The Head Chef creates the distributed DataFrame
df = spark.createDataFrame(data=data, schema=my_schema)
# --------------------------------------------------------------------
# --------------------------------------------------------------------
from pyspark.sql.functions import col

# Remember, these are TRANSFORMATIONS. Nothing executes yet!
engineers_df = df.filter(col("Department") == "Engineering")

# Let's add a new column that calculates a 10% bonus
bonus_df = engineers_df.withColumn("Bonus", col("Salary") * 0.10)

# Finally, an ACTION to trigger the execution
bonus_df.show()