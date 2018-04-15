from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import math

def devThread():
    def standDevH(data):
        H = 1.0
        if data.count():
            key_val_cnt = data
            keys_val_cnt = key_val_cnt.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
            total_cnt = keys_val_cnt.map(lambda x: x[1][1]).reduce(lambda x, y: x+y)
            total_val = keys_val_cnt.map(lambda x: x[1][0]).reduce(lambda x, y: x+y)
            averg = total_val/total_cnt
            print "total:", keys_val_cnt.collect()
            print "average:", averg
            keys_averg = keys_val_cnt.map(lambda x: (x[0], x[1][0]/x[1][1]))
            dev = math.sqrt(keys_averg.map(lambda x: (x[1] - averg)**2).reduce(lambda x, y: x+y)/total_cnt)
            print "dev:", dev
            if dev:
                devFromAverg = keys_averg.map(lambda x: (x[0], abs(x[1] - averg)/dev))
                devH = devFromAverg.filter(lambda x: x[1] > H)
                print "devH:", devH.collect()
    T = 5
    key_pos = 2 # src_pos = 2, protocal_pos = 4
    value_pos = 5
    words = data.map(lambda line: line.split(' ')).map(lambda record: (record[key_pos], float(record[value_pos])))
    # words = words.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 10, 2)
    processed_words = words.map(lambda x: (x[0], (x[1], 1)))
    processed_words.foreachRDD(standDevH)



sc = SparkContext("local[2]", "zns")
ssc = StreamingContext(sc, 1)
data = ssc.socketTextStream('localhost', 9999)
devThread()
ssc.start()
ssc.awaitTermination()


