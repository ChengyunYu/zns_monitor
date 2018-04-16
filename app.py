from flask import Flask, flash, redirect, render_template, \
     request, url_for
import random

queries = {}
sources = {}
app = Flask(__name__)

def geneQid():
    while 1:
        qid = random.randint(0, 1000)
        if qid not in queries:
            return qid

@app.route('/')
def my_form():
    return render_template('index.html', option_list=queries)

@app.route('/', methods=['POST'])
def my_form_post():

    print(request.form['action'])
    if request.form['action'] == 'input_submit':
        print (request.form['pro'])
        return redirect(url_for('my_form_post'))
    else:
        if request.form['action'] == 'query_submit':
            new_que = {}
            new_que['optid'] = geneQid()

            option_content = "Query for "+request.form['query_type'] + " for each "+request.form['time_unit']+"sec: "
            if request.form['topK']:
                new_que['num'] =  request.form['topK']
                new_que['type'] = 'topK'
                option_content += 'top-' +  request.form['topK']
            else:
                if request.form['devx']:
                    new_que['num'] =  request.form['devx']
                    new_que['type'] = 'devx'
                    option_content += request.form['devx']+r' times of Standard Dev'
                else:
                    new_que['num'] =  request.form['bandh']
                    new_que['type'] = 'bandh'
                    option_content += request.form['bandh']+r'% of Bandwith'
            new_que['value'] = option_content
            queries[new_que['optid']] = new_que
            return render_template('index.html', option_list=queries)
        else:
            del_qid = int(request.form.get('exist_queries'))
            if del_qid in queries:
                queries.pop(del_qid)
            return render_template('index.html', option_list=queries)

