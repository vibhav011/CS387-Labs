from pyspark.sql import SparkSession 
from pyspark.sql.functions import split as pyspark_split
import re

def reg_split(x):
    try:
        if not isinstance(x, str):
            x = str(x)
        rh = re.findall(r'(.*?)\s', x)[0]
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
    except:
        return [None, None, None, None, None]

def clean(x):
    print(x)
    return 0

def task_a(spark, csv_file):
    df = spark.read.text(csv_file)
    rdd = df.rdd
    return rdd

def task_b(rdd_a):
    rdd = rdd_a.map(lambda x: reg_split(x.value))
    cols = ['Remote Host', 'Request Timestamp', 'Request Method', 'Response Code', 'Response Length']
    df  = rdd.toDF(cols)
    rdd = df.rdd
    return rdd

def task_c(rdd_b):
    rdd_c = rdd_b.map(lambda x: clean(x))
    return rdd_c


def get_rdd(spark, csv_file):
    rdd_a = task_a(spark, csv_file)
    rdd_b = task_b(rdd_a)
    rdd_c = task_c(rdd_b)
    return rdd_c

if __name__ == "__main__":
    # print(reg_split('66.249.66.194 - - [22/Jan/2019:03:56:20 +0330] "GET /m/filter/b2,p6 HTTP/1.1" 200 19451 "-" "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.96 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)" "-"'))

    spark = SparkSession \
    .builder \
    .appName("PySpark create RDD example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()  

    csv_file = 'small.text'
    rdd_c = get_rdd(spark, csv_file)
    print(rdd_c.collect())

