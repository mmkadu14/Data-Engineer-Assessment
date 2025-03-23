# Databricks notebook source
df_financial_info = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("dbfs:/FileStore/Raw_data/finanical_information.csv")
df_client_details = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("dbfs:/FileStore/Raw_data/industry_client_details.csv")
df_payment_info = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("dbfs:/FileStore/Raw_data/payment_information.csv")
df_sub_info = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("dbfs:/FileStore/Raw_data/subscription_information.csv")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC #1.How many finance lending and blockchain clients does the organization have?
# MAGIC

# COMMAND ----------

df_count = df_client_details.filter((col('Industry')=="Finance Lending") | (col("Industry")== "Block Chain")) 


# COMMAND ----------

client_count = df_count.count()
print(f" The organisation have {client_count} Finance Lending and Block Chain Clients")

# COMMAND ----------

# MAGIC %md
# MAGIC #2.	Which industry in the organization has the highest renewal rate?
# MAGIC

# COMMAND ----------

join_df = df_sub_info.join(df_client_details,"client_id", "left")


# COMMAND ----------

renew_rate = join_df.groupBy("Industry").agg((count(when(col("renewed")== True,1 ))/count("*")*100).alias("renewal_rate"))


# COMMAND ----------

high_rate = renew_rate.orderBy(col("renewal_rate").desc()).first()
print(f"Highest Renewal rate: {high_rate['Industry']} ({high_rate['renewal_rate']:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3.	What was the average inflation rate when their subscriptions were renewed?
# MAGIC

# COMMAND ----------

df_avg_inflation = df_financial_info.alias("f").join(
    df_sub_info.alias("s"),(col("f.start_date") <= col("s.start_date")) & (col("f.end_date") >= col("s.start_date")), "inner")\
    .filter(col("s.renewed") == True)\
.agg(avg("f.inflation_rate").alias("avg_inflation_rate"))

df_avg_inflation.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 4.	What is the median amount paid each year for all payment methods? 
# MAGIC

# COMMAND ----------

df_median_payment = df_payment_info.withColumn("year", year("payment_date"))\
    .groupBy("year", "payment_method")\
    .agg(percentile_approx("amount_paid", 0.5).alias("median_amount_paid"))\
    .orderBy(col("year"))

df_median_payment.display()


# COMMAND ----------


