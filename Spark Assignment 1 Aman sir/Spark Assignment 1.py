# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Employee_info_1.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "Employee_info_1_csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from `Employee_info_1_csv`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "Employee_info_1_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# DBTITLE 1,Spark Assignment 1
"""
Assignment 1 Spark ~ Aman Sir

Create 2 data frame and and join them.

Create a dataframe and replace the null values of a column with some meaningful thing using withcolumn.

Note: Please use below csv to create data frame.

CSV FILES - Employee_info.csv & Employee_info_1.csv
"""

# COMMAND ----------

#Importing pyspark Lib.
import pyspark

#Making spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Assignment').getOrCreate()

#About Spark
spark

# COMMAND ----------

#Reading single File
df1 = spark.read.format("csv").option("inferSchema", True).option("header", True).option("sep",",").load("/FileStore/tables/Employee_info.csv")

display (df1)

print(df1.count())

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
schema_defined1 = StructType([StructField('Id', IntegerType(), True),
                             StructField('Name', StringType(),True),
                             StructField('Department', StringType(),True),
                            ])

# COMMAND ----------

df_a= spark.read.format("csv").schema(schema_defined1).option("header", True ).option("sep",",").load("/FileStore/tables/Employee_info.csv")
display (df_a)

# COMMAND ----------

#Reading single File
df2 = spark.read.format("csv").option("inferSchema", True).option("header", True).option("sep",",").load("/FileStore/tables/Employee_info_1.csv")

display (df2)

print(df2.count())

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
schema_defined2 = StructType([StructField('Id',IntegerType(), True),
                             StructField('Employee_id',IntegerType(),True),
                             StructField('City',StringType(),True),
                             StructField('State',StringType(),True),
                            ])

# COMMAND ----------

df_b= spark.read.format("csv").schema(schema_defined2).option("header", True ).option("sep",",").load("/FileStore/tables/Employee_info_1.csv")
display (df_a)

# COMMAND ----------

"""
Now we have two dataframes with defined schema df_a & df_b

"""

# COMMAND ----------

#join 'Full outer join'
df_join = df_a.join(df_b,df_a.Id == df_b.Id,"outer")
display(df_join)

# COMMAND ----------

#Replacing 'NUll' with meaningfull
from pyspark.sql.functions import regexp_replace
df_join.withColumn('City', regexp_replace('City', 'Null', 'Srinagar')) \
  .show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace
df_join.withColumn('Department', regexp_replace('Department', 'Null', 'Other')) \
  .show(truncate=False)

# COMMAND ----------


