from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import math
import thread
import time


def standardDevThread(key_pos, value_pos, H, T, query_no):
    def standDevX(data):
        key_val_cnt = data.filter(lambda x: x[1] > 0)
        if key_val_cnt.count():
            total_cnt = key_val_cnt.count()
            total_val = key_val_cnt.map(lambda x: x[1]).reduce(lambda x, y: x+y)
            averg = total_val/total_cnt
            print "total cnt:", total_cnt
            print "total:", key_val_cnt.collect()
            print "average:", averg
            dev = math.sqrt(key_val_cnt.map(lambda x: (x[1] - averg)**2).reduce(lambda x, y: x+y)/total_cnt)
            print "dev:", dev
            if dev:
                devFromAverg = key_val_cnt.map(lambda x: (x[0], abs(x[1] - averg)/dev))
                devH = devFromAverg.filter(lambda x: x[1] > H)
                print "devH:", devH.collect()
    sc = SparkContext("local[2]", "zns")
    ssc = StreamingContext(sc, 1)
    data = ssc.textFileStream(r"file:///Users/ChenNeng/Desktop/2018_Spring/Large_Data_Stream/project/data")
    ssc.checkpoint(r"file:///Users/ChenNeng/Desktop/2018_Spring/Large_Data_Stream/project/checkpoint%d"%query_no)
    words = data.map(lambda line: line.split(' ')).map(lambda record: (record[key_pos], float(record[value_pos])))
    processed_words = words.reduceByKeyAndWindow(lambda x, y: x+y, \
        lambda x, y: x-y, T, 2)
    processed_words.pprint()
    processed_words.foreachRDD(standDevX)
    ssc.start()
    ssc.awaitTermination()

def bandWidthThread(key_pos, value_pos, X, T, query_no):
    def bandH(data):
        key_val_cnt = data.filter(lambda x: x[1] > 0)
        if key_val_cnt.count():
            total_cnt = key_val_cnt.count()
            total_val = key_val_cnt.map(lambda x: x[1]).reduce(lambda x, y: x+y)
            moreThanX = key_val_cnt.map(lambda x: (x[0], x[1]/total_val)).filter(lambda x: x[1] > X)
            print "bandH:", moreThanX.collect()
    sc = SparkContext("local[2]", "zns")
    ssc = StreamingContext(sc, 1)
    data = ssc.textFileStream(r"file:///Users/ChenNeng/Desktop/2018_Spring/Large_Data_Stream/project/data")
    ssc.checkpoint(r"file:///Users/ChenNeng/Desktop/2018_Spring/Large_Data_Stream/project/checkpoint%d"%query_no)
    words = data.map(lambda line: line.split(' ')).map(lambda record: (record[key_pos], float(record[value_pos])))
    processed_words = words.reduceByKeyAndWindow(lambda x, y: x+y, \
        lambda x, y: x-y, T, 2)
    processed_words.pprint()
    processed_words.foreachRDD(bandH)
    ssc.start()
    ssc.awaitTermination()

def topKThread(key_pos, value_pos, K, T, query_no):
    def topK(data):
        key_val_cnt = data.filter(lambda x: x[1] > 0)
        if key_val_cnt.count():
            topk = key_val_cnt.top(K, key=lambda x: x[1])
            print "topk:", topk
    sc = SparkContext("local[2]", "zns")
    ssc = StreamingContext(sc, 1)
    data = ssc.textFileStream(r"file:///Users/ChenNeng/Desktop/2018_Spring/Large_Data_Stream/project/data")
    ssc.checkpoint(r"file:///Users/ChenNeng/Desktop/2018_Spring/Large_Data_Stream/project/checkpoint%d"%query_no)
    words = data.map(lambda line: line.split(' ')).map(lambda record: (record[key_pos], float(record[value_pos])))
    processed_words = words.reduceByKeyAndWindow(lambda x, y: x+y, \
        lambda x, y: x-y, T, 2)
    processed_words.pprint()
    processed_words.foreachRDD(topK)
    ssc.start()
    ssc.awaitTermination()

try:
    thread.start_new_thread(topKThread, (2, 5, 3, 6, 1))
except:
   print "Error: unable to start thread"

while 1:
    pass




