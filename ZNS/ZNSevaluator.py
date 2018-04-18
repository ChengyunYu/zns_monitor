from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import threading, os, sys
import math
import thread
import time
import multiprocessing, signal



class query(object):
    """docstring for query"""
    def __init__(self):
        self.num = 0
        self.value =''
        self.optid = 0
        self.content_type = ''
        self.query_type = ''
        self.T = 0
        self.proc = None


class ques(object):
    my_queries = {}
    def __init__(self):
        type(self).my_queries = {}

    def add_query(self, idx, new_query):
        print "add idx:", idx
        type(self).my_queries[idx] = new_query

    def delete_query(self, idx):
        print "delete_query:", idx
        type(self).my_queries[idx].proc.terminate()
        type(self).my_queries.pop(idx)

    def in_queries(self, idx):
        if idx in type(self).my_queries:
            return True
        return False

    def add_proc(self, idx, p):
        if not self.in_queries(idx):
            print "error idx for pid"
        else:
            type(self).my_queries[idx].proc = p

    def values(self):
        return type(self).my_queries.values()

queries = ques()

def newProcess(key_pos, value_pos, num, T, query_no, query_type):
    def standDevX(data):
        print ("in: ", query_no)
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

    def bandH(data):
        print ("in: ", query_no)
        key_val_cnt = data.filter(lambda x: x[1] > 0)
        if key_val_cnt.count():
            total_cnt = key_val_cnt.count()
            total_val = key_val_cnt.map(lambda x: x[1]).reduce(lambda x, y: x+y)
            moreThanX = key_val_cnt.map(lambda x: (x[0], x[1], x[1]/total_val)).filter(lambda x: x[2] > num)
            print("bandH:", moreThanX.collect())

    def topK(data):
        print ("in: ", query_no)
        key_val_cnt = data.filter(lambda x: x[1] > 0)
        if key_val_cnt.count():
            topk = key_val_cnt.top(num, key=lambda x: x[1])
            print("topk:", topk)

    conf = SparkConf().setAppName("zns").setMaster("local")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("off")
    ssc = StreamingContext(sc, 1)
    data = ssc.textFileStream(r"file:///Users/ChenNeng/Desktop/2018_Spring/Large_Data_Stream/project/data")
    ssc.checkpoint(r"file:///Users/ChenNeng/Desktop/2018_Spring/Large_Data_Stream/project/checkpoint%d"%query_no)
    words = data.map(lambda line: line.split(' ')).map(lambda record:
        (record[key_pos], float(record[value_pos])))
    processed_words = words.reduceByKeyAndWindow(lambda x, y: x+y, \
        lambda x, y: x-y, T, 2)
    processed_words.pprint()
    if(query_type == 'devx'):
        processed_words.foreachRDD(standDevX)
    else:
        if(query_type == 'bandh'):
            processed_words.foreachRDD(bandH)
        else:
            processed_words.foreachRDD(topK)

    ssc.start()
    ssc.awaitTermination()


def parseQuery(new_queue):
    print ('id:', new_queue.optid)
    if new_queue.content_type == 'IP':
        key_pos = 2
    else:
        if new_queue.content_type == 'Protocol':
            key_pos = 4
    value_pos = 5
    new_proc = multiprocessing.Process(target=newProcess, args=(key_pos, value_pos, int(new_queue.num), int(new_queue.T), new_queue.optid, new_queue.query_type))
    queries.add_proc(new_queue.optid, new_proc)
    new_proc.start()







