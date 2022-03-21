import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# importing pandas library
import pandas as pd
# importing matplotlib library
import matplotlib.pyplot as plt
from q2 import get_rdd

def part_a(df):
    with open('task-d/a.txt', 'w') as f:
        f.write("HTTP status analysis:\n")
        f.write(df.groupBy("Response Code").count().toPandas().to_string(index=False))
        
def part_b(df):
    df_temp = df.groupBy("Response Code").count().toPandas()
    tot = df.count()
    plt.pie(df_temp["count"]/tot, labels=df_temp["Response Code"], autopct='%.1f')
    plt.savefig('task-d/b.jpg')

def part_c(df):
    with open('task-d/c.txt', 'w') as f:
        f.write("Frequent Hosts:\n")
        f.write(df.groupBy("Remote Host").count().toPandas().to_string(index=False))

def part_d(df):
    with open('task-d/d.txt', 'w') as f:
        f.write("Unique hosts:\n")
        f.write(df.agg(countDistinct("Remote Host")).toPandas().to_string(index=False))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=str, default='a')
    args = parser.parse_args()

    spark = SparkSession \
    .builder \
    .appName("PySpark create RDD example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate() 
    csv_file = 'small.text'

    rdd = get_rdd(spark, csv_file)
    df = rdd.toDF()

    if args.part == 'a':
        part_a(df)
    elif args.part == 'b':
        part_b(df)
    elif args.part == 'c':
        part_c(df)
    elif args.part == 'd':
        part_d(df)
    elif args.part == 'all':
        part_a(df)
        part_b(df)
        part_c(df)
        part_d(df)