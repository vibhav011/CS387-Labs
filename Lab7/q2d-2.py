import argparse
from pyspark.sql import SparkSession
from q2 import get_rdd

def part_e(df):
    pass

def part_f(df):
    pass

def part_g(df):
    pass

def part_h(df):
    pass

def part_i(df):
    pass

def part_j(df):
    pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=str, default='e')
    args = parser.parse_args()

    spark = SparkSession \
    .builder \
    .appName("PySpark create RDD example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate() 
    csv_file = 'small.log'

    rdd = get_rdd(spark, csv_file)
    df = rdd.toDF()

    if args.part == 'e':
        part_e(df)
    elif args.part == 'f':
        part_f(df)
    elif args.part == 'g':
        part_g(df)
    elif args.part == 'h':
        part_h(df)
    elif args.part == 'i':
        part_i(df)
    elif args.part == 'j':
        part_j(df)