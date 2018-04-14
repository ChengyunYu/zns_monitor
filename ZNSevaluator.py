from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def standDevH(key_pos, value_pos, data, H):
    key_val_cnt = data.map(lambda record: (record[key_pos], (int(record[value_pos]), 1)))
    keys_val_cnt = key_val_cnt.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    total_cnt = keys_val_cnt.map(lambda x: x[1][1]).reduce(lambda x, y: x+y)
    total_val = keys_val_cnt.map(lambda x: x[1][0]).reduce(lambda x, y: x+y)
    total_cnt.pprint()
    total_val.pprint()






# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "zns")
ssc = StreamingContext(sc, 1)
data = ssc.textFileStream('file:///xxx/Large_Data_Stream/project/data')



# Deviation thread
words = data.window(3,3).map(lambda line: line.split(' ')) # tumblr window
standDevH(2, 5, words, 0.3)
ssc.start()
ssc.awaitTermination()

