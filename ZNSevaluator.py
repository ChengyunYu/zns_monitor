from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import math
import thread
import time


def standardDevThread(key_pos, value_pos, H, T, query_no):
    def standDevH(data):
        key_val_cnt = data.filter(lambda x: x[1][1] > 0)
        if key_val_cnt.count():
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
    sc = SparkContext("local[2]", "zns")
    ssc = StreamingContext(sc, 1)
    data = ssc.textFileStream(r"file:///Users/ChenNeng/Desktop/2018_Spring/Large_Data_Stream/project/data")
    ssc.checkpoint(r"file:///Users/ChenNeng/Desktop/2018_Spring/Large_Data_Stream/project/checkpoint%d"%query_no)
    words = data.map(lambda line: line.split(' ')).map(lambda record: (record[key_pos], (float(record[value_pos]), 1)))
    words.pprint()
    processed_words = words.reduceByKeyAndWindow(lambda x, y: (x[0]+y[0], x[1]+y[1]), \
        lambda x, y: (x[0]-y[0], x[1]-y[1]), T, 1)
    words.foreachRDD(standDevH)
    ssc.start()
    ssc.awaitTermination()

try:
    thread.start_new_thread(standardDevThread, (2, 5, 1, 5, 1))
except:
   print "Error: unable to start thread"

while 1:
    pass




