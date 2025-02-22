#!/usr/bin/env python3

import os
import sys
import json
import yaml
from flask import Flask, render_template, request, redirect, url_for, jsonify, send_from_directory, Response
from werkzeug.utils import secure_filename
from benchmarksql import BenchmarkSQL
import jproperties

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'config/faults'
bench = BenchmarkSQL()

faults = []

ROLES=["storage", "compute", "metadata"]

@app.route('/', methods = ['POST', 'GET'])
def index():
    return render_template('layout.html')

# 配置页面
@app.route('/config', methods=['GET', 'POST'])
def show_config():
    if request.method == 'POST':
        form = request.form

        action = request.form.get('action')
        
        # 处理表单配置
        if 'db' in request.form:
            config = {
                'db': request.form['db'],
                'driver': request.form['driver'],
                'application': request.form['application'],
                'conn': request.form['conn'],
                'user': request.form['user'],
                'password': request.form['password'],
                'warehouses': request.form['warehouses'],
                'loadWorkers': request.form['loadWorkers'],
                # 'monkeys': request.form['monkeys'],
                # 'sutThreads': request.form['sutThreads'],
                'runMins': request.form['runMins'],
                'reportScript': request.form['reportScript'],
                'sys.k8scli': request.form.get('sys.k8scli', ''),
                'sys.namespace': request.form.get('sys.namespace', ''),
                'sys.faults': request.form.get('sys.faults', ''),
                'sys.faulttime': request.form.get('sys.faulttime', ''),
            }
            
            # get all zones
            zones = []
            pods = []
            for k, v in request.form.items():
                if k.startswith('sys.zone') and k.endswith('name') and v:
                    zones.append(v)
            config['sys.zones'] = ','.join(zones)
            for zone in zones:
                config[f"sys.{zone}.pods"] = request.form.get(f"sys.{zone}.pods", '')
                pods.append(request.form.get(f"sys.{zone}.pods", ''))
            
            if request.form.get('sys.leaderzone', '') != '':
                config['sys.leaderzone'] = request.form.get('sys.leaderzone')
            
            config['sys.pods'] = ','.join(pods)
            
            # get all node types
            for role in ROLES:
                if request.form.get(f"sys.{role}.pods", '') != '':
                    config[f"sys.{role}.pods"] = request.form.get(f"sys.{role}.pods")
                    config[f"sys.{role}.volumePath"] = request.form.get(f"sys.{role}.volumePath")
            

        # debug info
        # print("action:", action, "config:", config, file=sys.stderr)

        # convert config to properties
        properties = jproperties.Properties()
        for k, v in config.items():
            properties[k] = v
        
        if 'action' in form:
            state = bench.get_job_type()

            prop = properties.store()

            if form['action'] == 'RUN' and state == 'IDLE':
                bench.save_properties(prop)
                bench.run_benchmark()
            
            elif form['action'] == 'RUNALL' and state == 'IDLE':
                bench.save_properties(prop)
                bench.run_allfaults()

            elif form['action'] == 'BUILD' and state == 'IDLE':
                bench.save_properties(prop)
                bench.run_build()

            elif form['action'] == 'DESTROY' and state == 'IDLE':
                bench.save_properties(prop)
                bench.run_destroy()

            elif form['action'] == 'CANCEL':
                bench.cancel_job()

            elif form['action'] == 'APPEND':
                bench.append_benchmark(prop)
        
        return redirect(url_for('show_results'))
    
    data = {}
    data['current_job_type'] = bench.get_job_type()
    data['current_job_runtime'] = bench.get_job_runtime()
    # data['form'] = form
    data['properties'] = bench.get_properties()

    if data['current_job_type'] == 'IDLE':
        data['state_run'] = ''
        data['state_build'] = ''
        data['state_destroy'] = ''
        data['state_cancel'] = 'disabled'
        data['state_refresh'] = ''
    else:
        data['state_run'] = 'disabled'
        data['state_build'] = 'disabled'
        data['state_destroy'] = 'disabled'
        data['state_cancel'] = ''
        data['state_refresh'] = ''

    data['current_job_output'] = bench.get_job_output()
    data['url_job_status'] = url_for('job_status')
    data['faults_list'] = get_cases()

    data['results'] = bench.get_results()
    return render_template('config.html', 
                         **data)

@app.route('/job_status')
def job_status():
    result = [
            bench.get_job_type(),
            bench.get_job_runtime(),
            bench.get_job_output(),
        ]
    return json.dumps(result)

# 结果页面
@app.route('/results')
def show_results():
    data = {}
    data['current_job_output'] = bench.get_job_output()
    data['job_status'] = job_status()
    data['results'] = bench.get_results()

    return render_template('results.html', **data)

# 故障管理
@app.route('/show_cases', methods=['GET', 'POST'])
def show_cases():
    if request.method == 'POST':
        if 'file' in request.files:
            file = request.files['file']
            if file.filename != '':
                filename = secure_filename(file.filename)
                file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        
        fault_data = {
            'name': request.form['name'],
            'type': request.form['type'],
            'params': {k: request.form[k] for k in request.form if k.startswith('param_')},
            'scope': {
                'zone_type': request.form['zone_type'],
                'role': request.form['role'],
                'pod_count': request.form['pod_count'],
                'duration': request.form['duration']
            }
        }
        save_fault(fault_data)
    
    return render_template('faults.html', 
                         fault_types=bench.FAULT_TYPES,
                         faults=get_cases(),
                         zone_types=["leader", "follower", "random", ""],
                         roles=ROLES)

@app.route('/add_case', methods=['POST'])
def add_case():
    # 从表单获取数据
    form = request.form
    zone_type = form['zone']
    if zone_type == "":
        zone_type = None
    role = form['role']
    pod_count = int(form['pod_count'])

    fault_type = form['type']
    fault_params = {}
    for k, v in form.items():
        if k.startswith('param_'):
            fault_params[k[6:]] = v
    duration = int(form['duration'])
    
    other_params = {}
    for k, v in form.items(): 
        if k.startswith('param_') or k in ['zone', 'role', 'pod_count', 'type', 'duration']:
            continue
        other_params[k] = v

    bench.generate_case_file(zone_type, role, pod_count, fault_type, duration, fault_params, other_params)
    
    return redirect(url_for('show_cases'))

@app.route('/delete_case/<case_name>')
def delete_case(case_name):
    bench.delete_case(case_name)
    return redirect(url_for('show_cases'))

@app.route('/show_case/<case_name>')
def show_case(case_name):
    return Response(bench.show_case(case_name), mimetype='text/plain')

# 辅助函数
def get_cases():
    return bench.get_cases()

def save_fault(data):
    filename = f"{data['name']}.yaml"
    with open(os.path.join(app.config['UPLOAD_FOLDER'], filename), 'w') as f:
        yaml.dump(data, f)

@app.route('/cancel_job')
def cancel_job():
    result = [
            bench.cancel_job(),
        ]
    return json.dumps(result)

@app.route('/result_log/')
def result_log():
    args = request.args
    return Response(bench.get_log(args['run_id']), mimetype='text/plain')

@app.route('/result_fault/')
def result_fault():
    args = request.args
    return Response(bench.get_fault(args['run_id']), mimetype='text/plain')

@app.route('/result_show/')
def result_show():
    args = request.args
    return bench.get_report(args['run_id'])

@app.route('/result_delete/')
def result_delete():
    args = request.args
    bench.delete_result(args['run_id'])
    return redirect(url_for("index"))

@app.route('/result_detail/')
def result_detail():
	args = request.args
	metric_datas = bench.get_allmetrics(args['run_id'])
	return render_template('result_detail.html', metric_datas=metric_datas)	

def upload_properties():
    print("files:", request.files, file=sys.stderr)
    pass

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5004, debug=True)