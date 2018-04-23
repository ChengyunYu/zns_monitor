#!/usr/bin/env python
# Created by Neng Chen
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import threading, os, sys
import math
import thread
import time, random
import multiprocessing, signal
import numpy as np


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


class queRes(object):
    def __init__(self):
        self.que_res = multiprocessing.Queue()


    def addRes(self, query_type, k_v):
        new_res = {'type':query_type, 'data' : []}
        if not k_v:
            return
        if len(k_v[0]) == 2:
            for (k, v) in k_v:
                new_res['data'].append({'name':k, 'num':v})
        else:
            for (k, v, dummy) in k_v:
                new_res['data'].append({'name':k, 'num':v})
        self.que_res.put(new_res)
        print("addRes:", new_res)

    def getRes(self):
        ret = []
        while not self.que_res.empty():
            ret.append(self.que_res.get())
        return ret




class ques(object):
    my_queries = {}
    def __init__(self):
        type(self).my_queries = {}


    def delete_query(self, idx):
        print("delete_query:", idx)
        type(self).my_queries[idx].proc.terminate()
        type(self).my_queries.pop(idx)

    def in_queries(self, idx):
        if idx in type(self).my_queries:
            return True
        return False

    def add_proc(self, idx, p):
        if not self.in_queries(idx):
            print("error idx for pid")
        else:
            type(self).my_queries[idx].proc = p

    def values(self):
        return type(self).my_queries.values()

    def geneQid(self):
        while 1:
            qid = random.randint(1, 1000)
            if not self.in_queries(qid):
                return qid

    def add_query(self, new_query):
        qid = self.geneQid()
        print("add idx:", qid)
        type(self).my_queries[qid] = new_query
        new_query.optid = qid

class inputStr(object):
    def __init__(self):
        self.idx = 0
        self.ips = ''
        self.ip_frac = ''
        self.pro = ''
        self.pro_frac = ''
        self.bandwidth = ''
        self.pack_size = ''
        self.pack_size_frac = ''

    def data_clean(self):
        def split_line(line):
            return np.array(line.split(), float)
        if(self.ip_frac):
            self.ip_frac = split_line(self.ip_frac)
            self.ips = self.ips.split()
        if(self.pro_frac):
            self.pro_frac = split_line(self.pro_frac)
            self.pro = self.pro.split()
        if(self.pack_size):
            self.pack_size = split_line(self.pack_size)
            self.pack_size_frac = split_line(self.pack_size_frac)
        if self.bandwidth:
            self.bandwidth = float(self.bandwidth)

    def print_out(self):
        print("Input str:")
        print(self.ips)
        print(self.ip_frac)
        print(self.pro)
        print(self.pro_frac)
        print(self.bandwidth)
        print(self.pack_size)
        print(self.pack_size_frac)

class dataStr(object):
    my_input_str = {}
    def __init__(self):
        type(self).my_input_str = {}

    def geneQid(self):
        while 1:
            qid = random.randint(1, 1000)
            if not self.my_input_str(qid):
                return qid

    def add_input_str(self, new_input):
        idx = self.geneQid()
        print("add input idx:", idx)
        type(self).my_input_str[idx] = new_input
        new_query.idx = idx


def newProcess(key_pos, value_pos, num, T, query_no, query_type, chart_res_que):
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
                devH_res = devH.collect()
                print('devH_res:', devH_res)
                chart_res_que.addRes(send_query_type[0], devH_res)


    def bandH(data):
        print ("in: ", query_no)
        key_val_cnt = data.filter(lambda x: x[1] > 0)
        if key_val_cnt.count():
            total_cnt = key_val_cnt.count()
            total_val = key_val_cnt.map(lambda x: x[1]).reduce(lambda x, y: x+y)
            moreThanX = key_val_cnt.map(lambda x: (x[0], x[1], x[1]/total_val)).filter(lambda x: x[2] > num)
            bandH_res = moreThanX.collect()
            print("bandH:", bandH_res)
            chart_res_que.addRes(send_query_type[0], bandH_res)

    def topK(data):
        print ("in: ", query_no)
        key_val_cnt = data.filter(lambda x: x[1] > 0)
        if key_val_cnt.count():
            topk = key_val_cnt.top(int(num), key=lambda x: x[1])
            print("topk:", topk)
            chart_res_que.addRes(send_query_type[0], topk)

    send_query_type =[query_type]
    conf = SparkConf().setAppName("zns").setMaster("local")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("off")
    ssc = StreamingContext(sc, 1)
    data = ssc.textFileStream(r"./Data")
    ssc.checkpoint(r"../checkpoint%d"%query_no)
    words = data.map(lambda line: line.split(' ')).map(lambda record:
        (record[key_pos], float(record[value_pos])))
    processed_words = words.reduceByKeyAndWindow(lambda x, y: x+y, \
        lambda x, y: x-y, T, 1)
    if(query_type == 'devx'):
        processed_words.foreachRDD(standDevX)
    else:
        if(query_type == 'bandh'):
            processed_words.foreachRDD(bandH)
        else:
            processed_words.foreachRDD(topK)
    ssc.start()
    ssc.awaitTermination()

def always_on_stat(chart_res_que):
    def __stat(data):
        print("in default")
        key_val_cnt = data.filter(lambda x: x[1] > 0)
        if key_val_cnt.count():
            total_vol = key_val_cnt.map(lambda x: x[1]).reduce(lambda x, y: x+y)
            word_frac = key_val_cnt.map(lambda x: (x[0], x[1]/total_vol)).collect()
            chart_res_que.addRes(my_argvs[0], word_frac)
    conf = SparkConf().setAppName("zns_def").setMaster("local")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("off")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint(r"../checkpoint0")
    data = ssc.textFileStream(r"./Data")
    tmp = data.map(lambda line: line.split(' '))
    word_ip = tmp.map(lambda record:
            (record[2], float(record[5]))).reduceByKeyAndWindow(lambda x, y: x+y, \
            lambda x, y: x-y, 10, 2)
    word_pro = tmp.map(lambda record:
            (record[4], float(record[5]))).reduceByKeyAndWindow(lambda x, y: x+y, \
            lambda x, y: x-y, 10, 2)
    my_argvs = ['default_ip']
    word_ip.foreachRDD(__stat)
    my_argvs = ['default_pro']
    word_pro.foreachRDD(__stat)
    ssc.start()
    ssc.awaitTermination()

queries = ques()
chart_res = queRes()
data_str = dataStr()
def_proc = multiprocessing.Process(target=always_on_stat, args=(chart_res,))
def_proc.start()


def parseQuery(new_queue):
    if new_queue.content_type == 'IP':
        key_pos = 2
    else:
        if new_queue.content_type == 'Protocol':
            key_pos = 4
    value_pos = 5
    print "type:", new_queue.query_type
    new_proc = multiprocessing.Process(target=newProcess, args=(key_pos, value_pos, float(new_queue.num), float(new_queue.T), new_queue.optid, new_queue.query_type, chart_res))
    queries.add_proc(new_queue.optid, new_proc)
    new_proc.start()
