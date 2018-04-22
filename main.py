from flask import Flask, flash, redirect, render_template, \
     request, url_for, Response
import random
import json
from ZNS import ZNSevaluator

app = Flask(__name__)



def Response_headers(content):
    resp = Response(content)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp

@app.route('/echarts-1')
def echarts_1_post():
    print('in echarts-1')
    ctrs =  ZNSevaluator.chart_res.getRes()
    print ctrs
    content = json.dumps(ctrs)
    resp = Response_headers(content)
    return resp


@app.route('/')
def my_form():
    return render_template('index.html', option_list=ZNSevaluator.queries.values())

@app.route('/', methods=['POST'])
def my_form_post():
    print(request.form['action'])
    if request.form['action'] == 'input_submit':
        new_data_str = ZNSevaluator.inputStr()
        if request.form['ips']:
            new_data_str.ips = request.form['ips']
            new_data_str.ip_frac = request.form['ip_frac']
        if request.form['pro']:
            new_data_str.pro = request.form['pro']
            new_data_str.pro_frac = request.form['pro_frac']
        if request.form['pack_size']:
            new_data_str.pack_size = request.form['pack_size']
            new_data_str.pack_size_frac = request.form['pack_size_frac']
        if request.form['bandwidth']:
            new_data_str.bandwidth = request.form['bandwidth']

        new_data_str.data_clean()
        new_data_str.print_out()
    else:
        if request.form['action'] == 'query_submit':
            new_que = ZNSevaluator.query()
            option_content = "Query for "+request.form['query_type'] + " for each "+request.form['time_unit']+" sec: "
            new_que.content_type = request.form['query_type']
            new_que.T = request.form['time_unit']
            if request.form['topk']:
                new_que.num =  request.form['topk']
                new_que.query_type = 'topk'
                option_content += 'top-' +  request.form['topk']
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
            ZNSevaluator.queries.add_query(new_que)
            ZNSevaluator.parseQuery(new_que)
        else:
            print "here"
            if 'exist_queries' in request.form:
                del_qid = int(request.form['exist_queries'])
                print "del_qid:", del_qid
                if ZNSevaluator.queries.in_queries(del_qid):
                    ZNSevaluator.queries.delete_query(del_qid)
    return render_template('index.html', option_list=ZNSevaluator.queries.values())

if __name__ == '__main__':
    app.run(debug=True)
