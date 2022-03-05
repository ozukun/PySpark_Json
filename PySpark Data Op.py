# Databricks notebook source
from pyspark.sql.types import *

# Read multiline json file 1
data_df = spark.read.option("multiline","false") \
      .json("/FileStore/tables/JSON/examples.json")
display(data_df) 

# COMMAND ----------

# directions columns contains Array lets try to flat it out...

#Using SQL col() function
from pyspark.sql.functions import col
data_df2= data_df["title","directions"].filter(col("title")=="Tacos")

display(data_df2)

from pyspark.sql.functions import explode
display(  data_df2.select(data_df2.title,explode(data_df2.directions))  ) # explode function used for flat it out...

data_df3=data_df2.select(data_df2.title,explode(data_df2.directions))

# COMMAND ----------

#Use array() function to create a new array column by merging the data from multiple columns.

from pyspark.sql.functions import array

display( data_df3.select(data_df3.title,array(data_df3.title,data_df3.col).alias("Dummy_array")) )

#display( data_df3.select(data_df3.title,array(data_df3.title,data_df3.col)[1].alias("Dummy_array")) )


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

data_schema = StructType(
  [
    StructField("Title",StringType(),True),
    StructField("Desc",ArrayType(StringType()),True)
  ]
)



# using StructType grammar we neet a list as data  thats y below conversion needed
import numpy as np
x=(data_df2.collect()) # collect retrieves all elements in a DataFrame as an Array
data_df4 = spark.createDataFrame(data=x,schema=data_schema) # we use Array x as data input -- we cant use another dataframe as input
data_df4.printSchema()
#display(data_df4)

print(x)
print(data_df4)

# COMMAND ----------


a1= data_df3.select(col("Title")).toPandas()['Title'].tolist() # instead of using collect to convert using toPandas then using tolist to convert from dataframe to list

print(type(a1))

# using collect func. bcz of retrieving all data , it can cause out of memory error in the case of big dataset

# COMMAND ----------

display(data_df3)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

data_schema = StructType(
  [
    StructField("Title",StringType(),True),
    StructField("Desc",StringType(),True)
  ]
)


y=data_df3.select(col("Title"),col('col')).toPandas().values.tolist() # instead of collect we use toPandas and values to covert dataframe into a list
data_df4 = spark.createDataFrame(data=y,schema=data_schema) # we use Array x as data input -- we cant use another dataframe as input
data_df4.printSchema()
#display(data_df4)

print(x)
display(data_df4)

# COMMAND ----------

#using MapType  as input
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, MapType

schema = StructType([
    StructField('name', StringType(), True),
    StructField('properties', MapType(StringType(),StringType()),True)
])

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
dataDictionary = [
        ('James',{'hair':'black','eye':'brown'}),
        ('Michael',{'hair':'brown','eye':None}),
        ('Robert',{'hair':'red','eye':'black'}),
        ('Washington',{'hair':'grey','eye':'grey'}),
        ('Jefferson',{'hair':'brown','eye':''})
        ]
df = spark.createDataFrame(data=dataDictionary, schema = schema)
df.printSchema()
df.show(truncate=False)


display( df.select(col('name') ,col('properties').eye.alias("eye") , col('properties').hair.alias("hair"))  )

# COMMAND ----------

#for loop implementation
print(type(df))
df2=df.select(col('name') ,col('properties').eye.alias("eye") , col('properties').hair.alias("hair"))
print(type(df2))
df3=df2.toPandas()
print(type(df3))

for index, row in df3.iterrows(): # itterrow only works with pandas.dataframe
    print("\n")
    print(index)
    print(row['name'], row['hair'])

# COMMAND ----------

x=(df2.collect())  # collect function can work with <class 'pyspark.sql.dataframe.DataFrame'>

#y=(df3.collect())  # if type is <class 'pandas.core.frame.DataFrame'> we cant use collect function

display(df2.sample(0.20) ) # to get 20% sample records

# COMMAND ----------

# working on Parquet File 

#Apache Parquet file is a columnar storage format available to any project in the Hadoop ecosystem
#While querying columnar storage, it skips the nonrelevant data very quickly, making faster query execution. 
#As a result aggregation queries consume less time compared to row-oriented databases.



data =[("James ","","Smith","36636","M",3000),
              ("Michael ","Rose","","40288","M",4000),
              ("Robert ","","Williams","42114","M",4000),
              ("Maria ","Anne","Jones","39192","F",4000),
              ("Jen","Mary","Brown","","F",-1)]
columns=["firstname","middlename","lastname","dob","gender","salary"]
dfp=spark.createDataFrame(data,columns)

# write into parq. file
dfp.write.mode('overwrite').parquet("/tmp/output/people.parquet")

# read from parq.
parDF=spark.read.parquet("/tmp/output/people.parquet")


display(parDF)

#execute as sql

parDF.createOrReplaceTempView("ParquetTable")
parkSQL = spark.sql("select * from ParquetTable where salary >= 4000 ")

#CREATE TABLE USING PARQ. FILE
spark.sql("CREATE or REPLACE TEMPORARY VIEW PERSON USING parquet OPTIONS (path \"/tmp/output/people.parquet\")")
spark.sql("SELECT * FROM PERSON").show()


#CREATE PARTITIONED PARQ. file
dfp.write.partitionBy("gender","salary").mode("overwrite").parquet("/tmp/output/people2.parquet")


# COMMAND ----------

#to_json() function is used to convert DataFrame columns MapType or Struct type to JSON string,,


#display(df)

print( df.printSchema()  )

from pyspark.sql.functions import to_json,col

df.withColumn("properties",to_json(col("properties"))).show(truncate=False)
