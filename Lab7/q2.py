from pyspark.sql import SparkSession 
from pyspark.sql.functions import split as pyspark_split
import re

spark = SparkSession \
.builder \
.appName("PySpark create RDD example") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()  
print(spark)
  
df = spark.read.text('./archive/access.log')
rdd1 = df.rdd

# split_col = pyspark_split(df, " ")
# df = df.withColumn('Remote Host', split_col.getItem(0))
# df = df.withColumn('Request Timestamp', split_col.getItem(1))
# df = df.withColumn('Request Method', split_col.getItem(2))
# df = df.withColumn('Response Code', split_col.getItem(3))
# df = df.withColumn('Response Length', split_col.getItem(4))
print(re.split(r'\s+(?=([^"]*"[^"]*")*[^"]*$)', '99.99.188.195 - - [25/Jan/2019:05:33:08 +0330] "GET /static/images/amp/blog.png HTTP/1.1" 200 3863 "https://www.zanbil.ir/m/article/616/%D8%B9%D9%84%D8%AA-%D8%AE%D9%88%D8%A7%D8%A8-%D8%B1%D9%81%D8%AA%D9%86%D8%8C%DA%AF%D8%B2%DA%AF%D8%B2%D8%8C%D8%A8%DB%8C-%D8%AD%D8%B3%DB%8C-%D9%88-%D9%85%D9%88%D8%B1-%D9%85%D9%88%D8%B1-%D8%B4%D8%AF%D9%86-%D8%A7%D9%86%DA%AF%D8%B4%D8%AA%D8%A7%D9%86-%D8%AF%D8%B3%D8%AA-%D9%88-%D8%AF%D8%B1%D9%85%D8%A7%D9%86-%D8%A2%D9%86" "Mozilla/5.0 (iPhone; CPU iPhone OS 12_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/71.0.3578.89 Mobile/15E148 Safari/605.1" "-"'))
rdd2 = rdd1.map(lambda x: re.split(r'\s+(?=([^"]*"[^"]*")*[^"]*$)', str(x)))




