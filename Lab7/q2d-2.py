import argparse
from email import header
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
from q2 import get_rdd

def changeFormat(x):
    month = {"Jan":"01", "Feb":"02", "Mar":"03", "Apr":"04", "May":"05", "Jun":"06", "Jul":"07", "Aug":"08", "Sep":"09", "Oct":"10", "Nov":"11", "Dec":"12"}
    u = str(x).split('/')
    return u[2][:4] + '-' + month[u[1]][:2] + '-' + u[0][:2]

def part_e(df, to_print=True):
    df = df.withColumn("Request Timestamp", df['Request Timestamp'].substr(2, 11)) \
        .groupBy("Request Timestamp").agg(countDistinct("Remote Host")).toPandas()
    
    df.rename(columns = {'Request Timestamp':'day', 'count(Remote Host)':'hosts'}, inplace = True)
    
    df['day2'] = df['day'].apply(changeFormat)
    df = df.sort_values('day2')
    df = df[['day', 'hosts']]
    if to_print:
        with open("task-d/e.txt", "w") as f:
            f.write("Unique hosts per day:\n")
            f.write(df.to_string(index=False))
    
    return df

def part_f(df):
    df = part_e(df, to_print=False)
    plot = df.plot(x="day", y="hosts", kind="line", title="No of unique hosts daily", xlabel="Day", ylabel="Hosts Count")
    fig = plot.get_figure()
    fig.savefig("task-d/f.jpg")

def part_g(df):
    df = df.filter(df['Response Code'] >= 400).groupBy('Remote Host').count().toPandas().sort_values('count', ascending=False).head(5)
    with open("task-d/g.txt", "w") as f:
        f.write("Failed HTTP Clients:\n")
        f.write(df[['Remote Host']].to_string(index=False, header=False))

def part_h(df):
    df = df.filter(df['Request Timestamp'].contains('22/Jan/2019'))\
           .withColumn("Hour", df['Request Timestamp'].substr(14, 2)) \
           .withColumn("IsBad", when(df['Response Code']>=400, 1).otherwise(0)) \
           .groupBy("Hour").agg({"Remote Host":"count", "IsBad":"sum"})
        
    df = df.withColumnRenamed("count(Remote Host)", "Total Requests")\
           .withColumnRenamed("sum(IsBad)", "Failed Requests").toPandas().sort_values('Hour')
    
    plot = df.plot(x="Hour", y=["Total Requests", "Failed Requests"], kind="line")
    fig = plot.get_figure()
    fig.savefig("task-d/h.jpg")

def part_i(df):
    w = Window.partitionBy("day").orderBy(col("count").desc())

    df = df.withColumn("day", df['Request Timestamp'].substr(2, 11)) \
           .withColumn("hour", format_string("%s:00",df['Request Timestamp'].substr(14, 2))) \
           .groupBy("day", "hour").count().withColumn("row",row_number().over(w))\
           .filter(col("row") == 1).drop("row").toPandas()
    
    df['day2'] = df['day'].apply(changeFormat)
    df = df.sort_values('day2')
    
    with open("task-d/i.txt", "w") as f:
        f.write("Active Hours:\n")
        f.write(df[['day', 'hour']].to_string(index=False, header=True))

def part_j(df):
    df = df.withColumn("Response Length", df["Response Length"].cast(IntegerType()))
    df1 = df.agg({"Response Length":"min"})
    df2 = df.agg({"Response Length":"max"})
    df3 = df.agg({"Response Length":"avg"})
    with open("task-d/j.txt", "w") as f:
        f.write("Response length statistics:\n")
        f.write("Minimum length\t"+str(df1.collect()[0]['min(Response Length)'])+'\n')
        f.write("Maximum length\t"+str(df2.collect()[0]['max(Response Length)'])+'\n')
        f.write("Average length\t"+str(df3.collect()[0]['avg(Response Length)']))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=str, default='e')
    args = parser.parse_args()

    spark = SparkSession \
    .builder \
    .appName("PySpark create RDD example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate() 
    csv_file = 'small.text'

    rdd = get_rdd(spark, csv_file)
    df = rdd.toDF()

    # df.write.parquet("q2d.parquet")

    # df = spark.read.parquet("q2d.parquet")
    # df.show()

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
    elif args.part == 'all':
        part_e(df)
        part_f(df)
        part_g(df)
        part_h(df)
        part_i(df)
        part_j(df)