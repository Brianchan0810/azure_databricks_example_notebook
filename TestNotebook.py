# Databricks notebook source
display(dbutils.fs.ls('/databricks-datasets/'))

# COMMAND ----------

dbutils.data.help()

# COMMAND ----------

dbutils.notebook.run('/Workspace/Users/cxb2000abc@gmail.com/Notebook2', 60)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import col, udf

# COMMAND ----------

df = spark.read.option('header', True).csv('dbfs:/FileStore/dataset/vgsales.csv')

# COMMAND ----------

df.where(col("Year")!="N/A").show()

# COMMAND ----------

df.where(col("Year").isNull()).show()

# COMMAND ----------

dbutils.data.summarize(df)

# COMMAND ----------

df.where(col("Year")!="N/A").drop('Global_Sales').withColumnRenamed("Name", "Game_Name").withColumn("Year", col("Year").cast(IntegerType())).show()

# COMMAND ----------

df.count()

# COMMAND ----------

df.select("Rank").distinct().count()

# COMMAND ----------

df = df.where(col("Year")!="N/A") \
       .drop("Rank", "Global_Sales") \
       .withColumnRenamed("Name", "Game_name") \
       .withColumn("Year", col("Year").cast(IntegerType()))

# COMMAND ----------

df.columns

# COMMAND ----------

df = df.unpivot(['Game_name','Platform',  'Year',  'Genre',  'Publisher'], ['NA_Sales', 'EU_Sales', 'JP_Sales', 'Other_Sales'], 'Region', 'Sales')

# COMMAND ----------

def extract_region(str):
    return str[0:-6]

extractRegionUDF = udf(lambda x: extract_region(x),StringType())

# COMMAND ----------

df.show()

# COMMAND ----------

df = df.withColumn("Region", extractRegionUDF(col("Region"))) \
       .withColumn("Sales", col("Sales").cast(FloatType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

max_sales = df.select(max(col("Sales"))).collect()[0][0]

# COMMAND ----------

df.where(col("Sales")==max_sales).show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.groupBy("Platform").agg(sum("Sales").alias("Sum_salary")).sort("Sum_salary", ascending=False).show()

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

yearWindow = Window.partitionBy('Year')
df.withColumn("Yearly_sales_sum", sum(col("Sales")).over(yearWindow)).show()

# COMMAND ----------

df.show()

# COMMAND ----------

df.write.format("delta").mode("overwrite").partitionBy("Year").save("/delta/game_sales")

# COMMAND ----------

df.write.format("delta").saveAsTable("GameSalesDelta")

# COMMAND ----------


