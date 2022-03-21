from pyspark.sql import SparkSession  

def make_pairs(lst):
    pairs = []
    try:
        for i in range(1, int(lst[0])):
            for j in range(i+1, int(lst[0])):
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

def val(x):
    return x[1]

spark = SparkSession \
.builder \
.appName("PySpark create RDD example") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()  
  
df = spark.read.csv('./new.csv')
rdd1 = df.rdd.flatMap(make_pairs) # 1.a
rdd21 = rdd1.map(lambda x: (x,1))
rdd2 = rdd21.reduceByKey(lambda x,y: x+y) # 1.b


rdd2.map(toList).toDF(['Item1', 'Item2', 'Count']).toPandas().to_csv('./count.csv', index=False)
top5_lst = rdd2.top(5, key=val)

for pair_counts in top5_lst: # 1.c
    print(pair_counts[0])

