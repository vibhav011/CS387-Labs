from pyspark.sql import SparkSession 
from pyspark.sql.functions import split as pyspark_split
import re

def reg_split(x):
    if not isinstance(x, str):
        x = str(x)
    rh = re.findall(r'(.*?)\s(?!-)', x)[0]
    x = re.sub(r'(.*?)\s(?!-)', '', x, 1)
    x = x.strip()
    # print(x)
    rt = re.findall(r'(\[.*?\])', x)[0]
    x = re.sub(r'(\[.*?\])', '', x, 1)
    x = x.strip()
    # print(x)
    rm = re.findall(r'"(.*?)"', x)[0]
    x = re.sub(r'"(.*?)"', '', x, 1)
    x = x.strip()
    # print(x)
    rc = re.findall(r'\s*(.*?)\s+', x)[0]
    x = re.sub(r'\s*(.*?)\s+', '', x, 1)
    x = x.strip()
    # print(x)
    rl = re.findall(r'\s*(.*?)\s+', x)[0]
    return [rh, rt, rm, rc, rl]

# print(reg_split('66.249.66.194 - - [22/Jan/2019:03:56:20 +0330] "GET /m/filter/b2,p6 HTTP/1.1" 200 19451 "-" "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.96 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)" "-"'))

spark = SparkSession \
.builder \
.appName("PySpark create RDD example") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()  
print(spark)
  
df = spark.read.text('small.log')
rdd1 = df.rdd

rdd2 = rdd1.map(lambda x: reg_split(x.value))

cols = ['Remote Host', 'Request Timestamp', 'Request Method', 'Response Code', 'Response Length']
df2  = rdd2.toDF(cols)

rdd3 = df2.rdd
print(rdd3.collect())

