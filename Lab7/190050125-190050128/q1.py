from pyspark.sql import SparkSession  

def make_pairs(lst):
    pairs = []
    try:
        num = int(lst[0])
        lst = lst[1:num+1]
        lst = list(set(lst))
        for i in range(0, num):
            for j in range(i+1, num):
                if lst[i] is not None and lst[j] is not None:
                    if lst[i] < lst[j]:
                        pairs.append((lst[i], lst[j])) 
                    if lst[i] > lst[j]:
                        pairs.append((lst[j], lst[i])) 
    except:
        pass
    return pairs

def toCSVLine(data):
  return ','.join(str(d) for d in data)

def toList(data):
    arr = []
    arr.append(data[0][0])
    arr.append(data[0][1])
    arr.append(data[1])
    return arr



spark = SparkSession \
.builder \
.appName("PySpark create RDD example") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()  
  
df = spark.read.csv('./groceries - groceries.csv')
rdd1 = df.rdd.flatMap(make_pairs) # 1.a
df2 = rdd1.toDF().groupBy('_1', '_2').count()
df2 = df2.orderBy('count',ascending=False)


df2.toPandas().to_csv('./count.csv', index=False, header=False) # 1.b
for item in df2.take(5):
    print("('", item._1, "','", item._2,"')", sep="") # 1.c