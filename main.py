from flask import Flask, flash, redirect, render_template, \
     request, url_for
import random
from ZNS import ZNSevaluator

app = Flask(__name__)
ZNSevaluator.init_queries()

def geneQid():
    while 1:
        qid = random.randint(1, 1000)
        if not ZNSevaluator.in_queries(qid):
            return qid

@app.route('/')
def my_form():
    ZNSevaluator.init_queries();
    return render_template('index.html', option_list=ZNSevaluator.queries)

@app.route('/', methods=['POST'])
def my_form_post():
    print(request.form['action'])
    if request.form['action'] == 'input_submit':
        print (request.form['pro'])
    else:
        if request.form['action'] == 'query_submit':
            new_que = ZNSevaluator.query()
            new_que.optid = geneQid()

            option_content = "Query for "+request.form['query_type'] + " for each "+request.form['time_unit']+" sec: "
            new_que.content_type = request.form['query_type']
            new_que.T = request.form['time_unit']
            if request.form['topK']:
                new_que.num =  request.form['topK']
                new_que.query_type = 'topK'
                option_content += 'top-' +  request.form['topK']
            else:
                if request.form['devx']:
                    new_que.num =  request.form['devx']
                    new_que.type = 'devx'
                    option_content += request.form['devx']+r' times of Standard Dev'
                else:
                    new_que.num =  request.form['bandh']
                    new_que.type = 'bandh'
                    option_content += request.form['bandh']+r'% of Bandwith'
            new_que.value = option_content
            ZNSevaluator.add_query(new_que.optid, new_que)
            ZNSevaluator.parseQuery(new_que)
        else:
            print "here"
            if 'exist_queries' in request.form:
                del_qid = int(request.form['exist_queries'])
                print "del_qid:", del_qid
                if ZNSevaluator.in_queries(del_qid):
                    ZNSevaluator.delete_query(del_qid)
    return render_template('index.html', option_list=ZNSevaluator.queries)

