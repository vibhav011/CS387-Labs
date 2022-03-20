import argparse
from pyspark.sql import SparkSession
# importing pandas library
import pandas as pd
# importing matplotlib library
import matplotlib.pyplot as plt
from q2 import get_rdd

def part_a(df):
    print(df.groupBy("Response Code").count().show())
    return

def part_b(df):
    df_temp = df.groupBy("Response Code").count().toPandas()
    tot = df.count()
    print(df_temp)

    plt.pie(df_temp["count"]/tot, labels=df_temp["Response Code"], autopct='%.1f')
    plt.show()


def part_c(df):
    

def part_d(df):
    pass

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