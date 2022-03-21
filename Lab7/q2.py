from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StringType
import matplotlib.pyplot as plt
import re, os, time

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
        x = re.sub(r'\"(.*?)\"', '', x, 1)
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
    host_pattern = re.compile("\d{1,3}\.\d{1,3}\.\d{1,3}.\d{1,3}")
    if not host_pattern.match(x[0]):
        # print("host not matching")
        return 1
    
    # print(x[1])
    time_pattern = re.compile("\[\d\d/[A-Z a-z][A-Z a-z][A-Z a-z]/\d\d\d\d:\d\d:\d\d:\d\d [\+ \-]\d\d\d\d\]")
    # print(re.findall("\[\d\d/[A-Z][a-z][a-z]/\d\d\d\d:\d\d:\d\d:\d\d [\+ \-]\d\d\d\d\]",x[1]))
    if not time_pattern.match(x[1]):
        # print("time not matching")
        return 1
    
    method_pattern = re.compile(r"[A-Z]+ .*")
    # print(x[2])
    # print(re.findall(r"\bGET\b|\bPOST\b|\bPUT\b|\bPATCH\b|\bDELETE\b .*", x[2]))
    if not method_pattern.match(x[2]):
        # print("method not matching")
        return 1

    response_pattern = re.compile(r"[1 2 3 4 5]\d\d")
    if not response_pattern.match(x[3]):
        # print("response not matching")
        return 1
    
    length_pattern = re.compile("\d+")
    if not length_pattern.match(x[4]):
        #print("length not matching")
        return 1
    
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
    rdd_temp = rdd_b.map(lambda x: clean(x))
    bad_count = rdd_temp.reduce(lambda x,y: x+y)
    rdd_c = rdd_b.filter(lambda x: not clean(x))
    print("Number of bad Rows : ", bad_count)
    return rdd_c


def get_rdd(spark, csv_file):
    rdd_a = task_a(spark, csv_file)
    rdd_b = task_b(rdd_a)
    rdd_c = task_c(rdd_b)
    return rdd_c

### TASK D PARTS ###

def part_a(df):
    with open('task-d/a.txt', 'w') as f:
        f.write("HTTP status analysis:\n")
        f.write(df.groupBy("Response Code").count().cache().toPandas().to_string(index=False))
        
def part_b(df):
    df_temp = df.groupBy("Response Code").count().toPandas()
    tot = df.count()
    plt.pie(df_temp["count"]/tot, labels=df_temp["Response Code"], autopct='%.1f')
    plt.savefig('task-d/b.jpg')

def part_c(df):
    with open('task-d/c.txt', 'w') as f:
        f.write("Frequent Hosts:\n")
        f.write(df.groupBy("Remote Host").count().cache().toPandas().to_string(index=False))

def part_d(df):
    with open('task-d/d.txt', 'w') as f:
        f.write("Unique hosts:\n")
        f.write(df.agg(countDistinct("Remote Host")).toPandas().to_string(index=False))

def changeFormat(x):
    month = {"Jan":"01", "Feb":"02", "Mar":"03", "Apr":"04", "May":"05", "Jun":"06", "Jul":"07", "Aug":"08", "Sep":"09", "Oct":"10", "Nov":"11", "Dec":"12"}
    u = str(x).split('/')
    return u[2][:4] + '-' + month[u[1]][:2] + '-' + u[0][:2]

def part_e(df, to_print=True):
    df = df.withColumn("Request Timestamp", df['Request Timestamp'].substr(2, 11)) \
        .groupBy("Request Timestamp").cache().agg(countDistinct("Remote Host")).toPandas()
    
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
    df = df.filter(df['Response Code'] >= 400).groupBy('Remote Host').count().cache().toPandas().sort_values('count', ascending=False).head(5)
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
           .withColumn("hour", format_string("%02d:00",df['Request Timestamp'].substr(14, 2).cast(IntegerType())+1)) \
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
    # print(reg_split('66.249.66.194 - - [22/Jan/2019:03:56:20 +0330] "GET /m/filter/b2,p6 HTTP/1.1" 200 19451 "-" "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.96 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)" "-"'))
    t1 = time.time()
    spark = SparkSession \
    .builder \
    .appName("PySpark create RDD example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()  

    # csv_file = 'archive/access.log'
    csv_file = '/Users/vibhavaggarwal/Downloads/access.log'
    rdd_c = get_rdd(spark, csv_file)

    df = rdd_c.toDF()
    t2 = time.time()
    print(t2-t1)
    if not os.path.exists('task-d'):
        os.makedirs('task-d')
    part_a(df)
    t3 = time.time()
    print(t3-t2)
    part_b(df)
    # part_c(df)
    # part_d(df)
    # part_e(df)
    # part_f(df)
    # part_g(df)
    # part_h(df)
    # part_i(df)
    # part_j(df)
    print(time.time()-t3)