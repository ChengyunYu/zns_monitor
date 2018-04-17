from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import threading, os, sys
import math
import thread
import time


def init_queries():
    global queries
    print "init queries"
    queries = {}

def add_query(idx, new_query):
    print "add idx:", idx
    queries[idx] = new_query

def delete_query(idx):
    print "delete_query:", idx
    queries.pop(idx)

def in_queries(idx):
    if idx in queries:
        return True
    return False


class query(object):
    """docstring for query"""
    def __init__(self):
        self.num = 0
        self.value =''
        self.optid = 0
        self.content_type = ''
        self.query_type = ''
        self.T = 0


thread_pool = {}
class myThread(threading.Thread):
    def __init__(self, key_pos =0, value_pos =0, H=0, T=0, query_no=0, query_type=''):
        super(myThread, self).__init__()
        self.key_pos = key_pos
        self.value_pos = value_pos
        self.num = H
        self.T = T
        self.query_no = query_no
        self.type = query_type
    def run(self):
        if self.type == 'topK':
            self.topKThread(self.key_pos, self.value_pos, self.num, self.T, self.query_no)
        else:
            if self.type == 'bandh':
                self.bandWidthThread(self.key_pos, self.value_pos, self.num, self.T, self.query_no)
            else:
                self.standardDevThread(self.key_pos, self.value_pos, self.num, self.T, self.query_no)

    @staticmethod
    def standardDevThread(key_pos, value_pos, num, T, query_no):
        def standDevX(data):
            if not in_queries(query_no):
                return
            key_val_cnt = data.filter(lambda x: x[1] > 0)
            if key_val_cnt.count():
                total_cnt = key_val_cnt.count()
                total_val = key_val_cnt.map(lambda x: x[1]).reduce(lambda x, y: x+y)
                averg = total_val/total_cnt
                print("total cnt:", total_cnt)
                print("total:", key_val_cnt.collect())
                print("average:", averg)
                dev = math.sqrt(key_val_cnt.map(lambda x: (x[1] - averg)**2).reduce(lambda x, y: x+y)/total_cnt)
                print("dev:", dev)
                if dev:
                    devFromAverg = key_val_cnt.map(lambda x: (x[0], x[1], abs(x[1] - averg)/dev))
                    devH = devFromAverg.filter(lambda x: x[2] > num)
                    print("devH:", devH.collect())
        sc = SparkContext("local[2]", "zns")
        ssc = StreamingContext(sc, 1)
        data = ssc.textFileStream(r"file:///Users/ChenNeng/Desktop/2018_Spring/Large_Data_Stream/project/data")
        ssc.checkpoint(r"file:///Users/ChenNeng/Desktop/2018_Spring/Large_Data_Stream/project/checkpoint%d"%query_no)
        words = data.map(lambda line: line.split(' ')).map(lambda record:
            (record[key_pos], float(record[value_pos])))
        processed_words = words.reduceByKeyAndWindow(lambda x, y: x+y, \
            lambda x, y: x-y, T, 2)
        processed_words.pprint()
        processed_words.foreachRDD(standDevX)
        ssc.start()
        ssc.awaitTermination()

    @staticmethod
    def bandWidthThread(key_pos, value_pos, num, T, query_no):
        def bandH(data):
            if not in_queries(query_no):
                return
            key_val_cnt = data.filter(lambda x: x[1] > 0)
            if key_val_cnt.count():
                total_cnt = key_val_cnt.count()
                total_val = key_val_cnt.map(lambda x: x[1]).reduce(lambda x, y: x+y)
                moreThanX = key_val_cnt.map(lambda x: (x[0], x[1], x[1]/total_val)).filter(lambda x: x[2] > num)
                print("bandH:", moreThanX.collect())
        sc = SparkContext("local[2]", "zns")
        ssc = StreamingContext(sc, 1)
        data = ssc.textFileStream(r"file:///Users/ChenNeng/Desktop/2018_Spring/Large_Data_Stream/project/data")
        ssc.checkpoint(r"file:///Users/ChenNeng/Desktop/2018_Spring/Large_Data_Stream/project/checkpoint%d"%query_no)
        words = data.map(lambda line: line.split(' ')).map(lambda record:
            (record[key_pos], float(record[value_pos])))
        processed_words = words.reduceByKeyAndWindow(lambda x, y: x+y, \
            lambda x, y: x-y, T, 2)
        processed_words.pprint()
        processed_words.foreachRDD(bandH)
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")
        ssc.start()
        ssc.awaitTermination()

    @staticmethod
    def topKThread(key_pos, value_pos, num, T, query_no):
        def topK(data):
            if not in_queries(query_no):
                return
            key_val_cnt = data.filter(lambda x: x[1] > 0)
            if key_val_cnt.count():
                topk = key_val_cnt.top(num, key=lambda x: x[1])
                print("topk:", topk)
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


def parseQuery(new_queue):
    print ('id:', int(new_queue.optid))
    if in_queries(new_queue.optid):
        print 'in queries'
    else:
        print 'not in queries'
    if new_queue.content_type == 'IP':
        key_pos = 2
    else:
        if new_queue.content_type == 'Protocol':
            key_pos = 4
    value_pos = 5
    newThread = myThread(key_pos, value_pos, int(new_queue.num), int(new_queue.T), int(new_queue.optid), new_queue.query_type)
    thread_pool[int(new_queue.optid)] = newThread
    # newThread.start()


if __name__ == '__main__':
    try:
        thread.start_new_thread(standardDevThread, (2, 5, 1, 6, 1))
    except:
       print("Error: unable to start thread")

    while 1:
        pass




