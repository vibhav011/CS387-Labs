from pyspark.sql import SparkSession  

def make_pairs(lst):
    pairs = []
    for i in range(len(lst)):
        for j in range(i+1, len(lst)):
            if lst[i] is not None and lst[j] is not None:
                if lst[i] < lst[j]:
                    pairs.append((lst[i], lst[j])) 
                if lst[i] > lst[j]:
                    pairs.append((lst[j], lst[i])) 
    return pairs

def toCSVLine(data):
  return ','.join(str(d) for d in data)

def val(x):
    return x[1]

spark = SparkSession \
.builder \
.appName("PySpark create RDD example") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()  
print(spark)
  
df = spark.read.csv('./archive/groceries.csv')
rdd1 = df.rdd.flatMap(make_pairs)
#print(rdd1.keys().collect())
rdd21 = rdd1.map(lambda x: (x,1))
#print(rdd21.values().collect())
rdd2 = rdd21.reduceByKey(lambda x,y: x+y)

lines = rdd2.map(toCSVLine)
lines.toDF().toPandas().to_csv('./count.csv')
top5_lst = rdd2.top(5, key=val)

for pair_counts in top5_lst:
    print(pair_counts[0])

