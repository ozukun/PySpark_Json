# Read multiline json file 1
multiline_df = spark.read.option("multiline","false") \
      .json("/FileStore/tables/JSON/examples.json")
display(multiline_df) 



x=multiline_df[["_id","directions"]]


from pyspark.sql.functions import arrays_zip, col, explode


a=x.withColumn("tmp",arrays_zip("directions"))\
.withColumn("tmp", explode("tmp"))\
.select(col("_id"),col("tmp.directions").alias("dir"))

display(a)


y=(  multiline_df[["_id","desc","ingredients"]]  )
display(y)
y2=y.collect()
print(y2) 


from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

data_schema = StructType(fields=[
    StructField('_id', ( StructType([ StructField('x', StringType(), True) ]) ), True),
    StructField('desc', StringType(), True),
    StructField(
        'ingredients', ArrayType(
            StructType([
                StructField('name', StringType(), True),
                StructField('quantity', ( StructType([StructField('amount', IntegerType(), True),StructField('unit', StringType(), True)]) ), True) 
               
                
            ])
        )
    )
]
)

data_df1 = spark.createDataFrame(data=y2, schema=data_schema)


display(data_df1)

display( data_df1.select(col("_id").x.alias("ID"),col("desc").alias("DESC"),col("ingredients").quantity.amount.alias("Amount")) )




from pyspark.sql.functions import arrays_zip, col, explode


data_df2=data_df1.select(col("_id").x.alias("ID"),col("desc").alias("DESC"),col("ingredients").quantity.amount.alias("Amount"))

b=data_df2.withColumn("tmp",arrays_zip("Amount"))\
.withColumn("tmp", explode("tmp"))\
.select(col("ID"),col("DESC"),col("tmp.Amount").alias("Amount"))

display(b)

data_df_dummy=data_df1[["_id","ingredients"]]
display(data_df_dummy)
c=data_df_dummy.withColumn("tmp",arrays_zip("ingredients"))\
  .withColumn("tmp", explode("tmp"))\
  .select(col("_id.x"),col("tmp.ingredients.name"),col("tmp.ingredients.quantity.amount"),col("tmp.ingredients.quantity.unit"))


display(c)
