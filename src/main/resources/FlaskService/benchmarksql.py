# ----
# BenchmarkSQL.py - Control interface to run BenchmarkSQL components
# ----

# -*- coding: utf-8

import codecs
import json
import os
import shutil
import signal
import subprocess
import threading
import time
import csv
import shlex
import jproperties
import queue
import yaml
import sqlite3

class BenchmarkSQL:
    """
    Control interface for running BenchmarkSQL components.
    """
    def __init__(self):
        """
        Initialize the instance
        """

        self.FAULT_TYPES = {
            "fail": {"params": []},
            "io_fault": {"params": ["percent"]},
            "net_delay": {"params": ["latency"]},
            "net_loss": {"params": ["loss"]}
        }
        
        # ----
        # Set the path where we find the BenchmarkSQL run components.
        # We also make this our current directory for launching the
        # actual job scripts.
        # ----
        head, tail = os.path.split(__file__)
        self.run_dir = os.path.abspath(os.path.join(head, '..'))
        if not os.path.exists(os.path.join(self.run_dir, 'runBenchmark.sh')):
            raise Exception("BenchmarkSQL run components not found at '{0}'".format(self.run_dir))
        os.chdir(self.run_dir)

        # ----
        # The "service_data" directory defaults to ./service_data.
        # It that does not exist we assume to live in the docker container
        # and try /service_data.
        # ----
        self.data_dir = os.path.abspath(os.path.join(os.path.curdir, 'service_data'))
        # ----
        # faults_dis is the directory where the fault templates are stored.
        # ---- 
        self.faults_dis = os.path.abspath(os.path.join(os.path.curdir, 'FaultTemplates'))
        if os.path.isdir(self.data_dir):
            print("using existing", self.data_dir)
        else:
            if os.path.isdir('/service_data'):
                self.data_dir = '/service_data'
                print("assuming docker container, using", self.data_dir)
            else:
                os.mkdir(self.data_dir)
                print("created empty directory", self.data_dir)

        self.status_file = os.path.join(self.data_dir, 'status.json')
        self.status_data = self.load_status()

        self.lock = threading.Lock()
        self.current_job_type = 'IDLE'
        self.current_job = None
        self.current_job_id = 0
        self.current_job_name = ""
        self.current_sub_job_name = ""
        self.current_job_output = ""
        self.current_job_start = 0.0
        self.finished_reson = ""
        self.current_job_properties = self.get_properties()

		# pending properties
        self.pending_lock = threading.Lock()
        self.pending_properties = queue.Queue(maxsize=30)
        # create a thread to handle pending properties
        self.pending_thread = threading.Thread(target=self.handle_pending_benchmark, daemon=True).start()
    
    def handle_pending_benchmark(self):
        while True:
            self.lock.acquire()
            if self.current_job_type != 'IDLE':
                self.lock.release()
                time.sleep(1)
                continue
            self.lock.release()
            self.pending_lock.acquire()
            if not self.pending_properties.empty():
                prop = self.pending_properties.get()
                self.save_properties(prop)
                self.run_benchmark()
            self.pending_lock.release()
            time.sleep(1)
        
    def append_benchmark(self, properties):
        self.pending_lock.acquire()
        self.pending_properties.put(properties)
        self.pending_lock.release()

    def load_status(self):
        """
        Load the current status data from the /data/status.json file
        """
        if os.path.exists(self.status_file):
            with open(self.status_file, 'r') as fd:
                data = json.loads(fd.read())
            return data
        else:
            return {
                'run_count':    0,
                'results':      [],
                'filename':     'default.properties',
            }

    def save_status(self):
        """
        Save the current status data into the /data/status.json file
        """
        # ----
        # Caller should hold lock
        # ----
        with open(self.status_file, 'w') as fd:
            fd.write(json.dumps(self.status_data, indent = 4))

    def get_status(self):
        self.get_job_type()
        self.lock.acquire()
        result = {
                'current_job_type': self.current_job_type,
                'current_job_id': self.current_job_id,
                'current_job_name': self.current_job_name,
                'current_job_output': self.current_job_output,
                'current_job_start': self.current_job_start,
                'current_job_properties': self.current_job_properties,
            }
        self.lock.release()
        return result

    def get_job_type(self):
        self.lock.acquire()
        if self.current_job is not None:
            self.current_job.join(0.0)
            if not self.current_job.is_alive():
                for entry in self.status_data['results']:
                    if entry['name'] == self.current_job_name:
                        if entry['state'] == 'RUN':
                            entry['state'] = 'FINISHED'
                        if entry['state'] == 'ALLRUNNING':
                            entry['state'] = 'ALLFINISHED'
                        break
                self.current_job = None
                self.current_job_id = 0
                self.current_job_type = 'IDLE'
                self.current_job_name = ""
        result = self.current_job_type
        self.save_status()
        self.lock.release()
        return result

    def get_job_runtime(self):
        self.lock.acquire()
        if self.current_job is None:
            result = "--:--:--"
        else:
            runtime = int(time.time() - self.current_job_start)
            result =  "{0:02d}:{1:02d}:{2:02d}".format(
                      int(runtime / 3600), int((runtime / 60) % 60), int(runtime % 60))
        self.lock.release()
        return result

    def get_job_output(self):
        self.lock.acquire()
        result = self.current_job_output
        self.lock.release()
        return result

    def add_job_output(self, output):
        self.lock.acquire()
        self.current_job_output += str(output)
        self.lock.release()

    def get_job_txsummary(self, run_id):
        self.lock.acquire()
        try:
            fname = os.path.join(self.data_dir, "result_{0:06d}".format(run_id), "data", "summary.csv")
            result = {}
            with open(fname, 'r') as fd:
                csv_reader = csv.DictReader(fd)
                for row in csv_reader:
                    result[row['ttype']] = {key: row[key].rstrip('s%') for key in row.keys() if key != 'ttype'}
        except Exception as e:
            self.lock.release()
            raise e
        self.lock.release()
        return result

    def get_properties(self):
        self.lock.acquire()
        last_path = os.path.join(self.data_dir, 'last.properties')
        if os.path.exists(last_path):
            with open(last_path, 'r') as fd:
                result = fd.read()
        else:
            sample_path = os.path.join(os.path.dirname(__file__), 'sample.last.properties')
            with open(sample_path, 'r') as fd:
                result = fd.read()
        self.current_job_properties = result
        self.lock.release()
        return result

    def get_results(self):
        self.lock.acquire()
        results = []
        for entry in self.status_data['results']:
            result_dir = os.path.join(self.data_dir, entry['name'])
            results.append(
                (
                    entry['run_id'],
                    entry['name'],
                    entry['start'],
                    entry['state'],
                )
            )
        self.lock.release()
        return results

    def get_cases(self):
        # Get the list of fault templates, each fault contain a name and type
        cases = []
        fault_files = [f for f in os.listdir(self.faults_dis) if os.path.isfile(os.path.join(self.faults_dis, f))]
        for fault_file in fault_files:
            if 'io_fault' in fault_file:
                fault_type = 'io_fault'
            elif 'net_delay' in fault_file:
                fault_type = 'net_delay'
            elif 'net_loss' in fault_file:
                fault_type = 'net_loss'
            elif 'fail' in fault_file:
                fault_type = 'fail'
            else:
                fault_type = 'unknown'
            cases.append({'name': fault_file, 'type': fault_type})
        cases = sorted(cases, key=lambda x: x['name'])
        return cases
        
    def delete_case(self, case_name):
        # Delete the fault template file
        case_path = os.path.join(self.faults_dis, case_name)
        if os.path.exists(case_path):
            os.remove(case_path)
        return

    def show_case(self, case_name):
        # Show the content of the fault template file
        case_path = os.path.join(self.faults_dis, case_name)
        if os.path.exists(case_path):
            with open(case_path, 'r') as fd:
                return fd.read()
        return "Not found"
    
    
    # 定义故障位置生成函数
    def generate_injectpods(zone_type, role, pod_count):
        if zone_type == "leader":
            return f"$zone.leader-{role}-{pod_count}"
        elif zone_type == "follower":
            return f"$zone.follower.1-{role}-{pod_count}"
        elif zone_type == "random":
            return f"$zone.random-{role}-{pod_count}"
        elif zone_type == "":
            return f"${role}-{pod_count}"  # 不指定zone
        else:
            raise ValueError(f"Invalid zone_type: {zone_type}")

    def generate_case_file(self, zone_type, role, pod_count, fault_type, duration, fault_params, other_params):
        injectpods = self.generate_injectpods(zone_type, role, pod_count)
        fault_config = self.FAULT_TYPES[fault_type]
        config = {
            "template": fault_config["template"],
            "injectpods": injectpods,
            "duration": f"{duration}s"
        }

        # 将io故障路径设置为对应role的路径， 如storage角色设置为storage.volumnPath
        if fault_type == "io_fault":
            config["volumePath"] = "$" + role + ".volumePath"
        
        # 添加故障类型特定的参数
        for param, value in fault_params.items():
            config[param] = value
        
        for param, value in other_params.items():
            config[param] = value
        
        # 生成文件名
        file_name_parts = []
        if zone_type:  # 如果指定了zone，才将zone描述加入文件名
            file_name_parts.append(zone_type)
            file_name_parts.append("zone")
        file_name_parts.extend([role, "all" if pod_count == 0 else "one", fault_type])
        
        # 将故障参数添加到文件名中，并统一参数位数
        for param, value in fault_params.items():
            if param == "percent":
                formatted_value = f"{int(value):03d}"  # 百分比统一为3位数，例如 100 -> 100, 80 -> 080
            elif param == "latency":
                # 提取数字部分并统一为3位数，例如 1ms -> 001ms, 16ms -> 016ms
                num_value = ''.join(filter(str.isdigit, value))
                formatted_value = f"{int(num_value):03d}ms"
            elif param == "loss":
                formatted_value = f"{int(value):03d}"  # 丢包率统一为3位数，例如 5 -> 005, 20 -> 020
            else:
                formatted_value = str(value)  # 其他参数保持不变
            file_name_parts.append(f"{param}_{formatted_value}")
        
        file_name = "_".join(file_name_parts) + ".yaml"
        
        # 写入YAML文件到faults_dis
        file_path = os.path.join(self.faults_dis, file_name)
        with open(file_path, 'w') as file:
            yaml.dump(config, file, default_flow_style=False)
        
        print(f"Generated {file_name}")
    
    def save_properties(self, properties):
        self.lock.acquire()
        last_path = os.path.join(self.data_dir, 'last.properties')
        with open(last_path, 'w') as fd:
            fd.write(properties)
        self.current_job_properties = properties
        self.lock.release()

    def get_report(self, run_id):
        try:
            run_id = int(run_id)
        except Exception as e:
            return "What?"

        html_path = os.path.join(self.data_dir, "result_{0:06d}.html".format(run_id))
        try:
            with open(html_path, 'r') as fd:
                report = fd.read()
        except Exception as e:
            return str(e)
        return report

    def get_log(self, run_id):
        try:
            run_id = int(run_id)
        except Exception as e:
            return "What?"

        html_path = os.path.join(self.data_dir, "result_{0:06d}".format(run_id), 'console.log')
        try:
            with open(html_path, 'r') as fd:
                log = fd.read()
        except Exception as e:
            return str(e)
        return log

    def get_fault(self, run_id):
        try:
            run_id = int(run_id)
        except Exception as e:
            return "What?"

        html_path = os.path.join(self.data_dir, "result_{0:06d}".format(run_id), 'data/fault.yaml')
        try:
            with open(html_path, 'r') as fd:
                log = fd.read()
        except Exception as e:
            return str(e)
        return log
    
    def get_allmetrics(self, run_id):
        try:
            run_id = int(run_id)
        except Exception as e:
            return "What?"

        # get start_run_id and end_run_id from status_data
        current_status_data = [entry for entry in self.status_data['results'] if entry['run_id'] == run_id ][0]
        start_run_id = int(current_status_data['start_run_id'])
        end_run_id = int(current_status_data['end_run_id'])
        metric_datas = []
        # recursively read info from result_dir for each run_id
        for run_id in range(start_run_id, end_run_id):
            result_dir = os.path.join(self.data_dir, "result_{0:06d}".format(run_id))
            if not os.path.exists(result_dir):
                continue
            metric_data = {}
            metric_data['run_id'] = run_id
            # read fault name from result_dir/data/faultInfo.csv
            with open(os.path.join(result_dir, 'data', 'faultInfo.csv'), 'r') as fd:
                # name,start,end,duration
                # ./faults/storage_all_net_delay_latency_1ms.yaml,1737911387618,1737911517922,120000
                csv_reader = csv.DictReader(fd)
                for row in csv_reader:
                    fault_name = row['name']
                    metric_data['fault_name'] = fault_name
                    break
			# continue if fault_name is not found
            if 'fault_name' not in metric_data:
                continue
            # set fault_type according to fault_name
            # if fault_name contains 'io_fault', then fault_type is 'io_fault'
            # if fault_name contains 'net_delay', then fault_type is 'net_delay'
            # if fault_name contains 'net_loss', then fault_type is 'net_loss'
            # if fault_name contains 'fail', then fault_type is 'fail'
            if 'io_fault' in fault_name:
                metric_data['fault_type'] = 'io_fault'
            elif 'net_delay' in fault_name:
                metric_data['fault_type'] = 'net_delay'
            elif 'net_loss' in fault_name:
                metric_data['fault_type'] = 'net_loss'
            elif 'fail' in fault_name:
                metric_data['fault_type'] = 'fail'
            elif 'cpu_stress' in fault_name:
                metric_data['fault_type'] = 'cpu_stress'
            else:
                metric_data['fault_type'] = 'unknown'
            # read metrics from result_dir/data/metrics.csv
            with open(os.path.join(result_dir, 'data', 'metrics.csv'), 'r') as fd:
                # rto,rpo,recovery_time_factor,total_performance_factor,absorption_factor,recovery_factor
                # 0.0,239640,0,-1,-1,0.9889908256880735
                csv_reader = csv.DictReader(fd)
                for row in csv_reader:
                    metric_data['rpo'] = row['rpo']
                    metric_data['rto'] = row['rto']
                    metric_data['recovery_time_factor'] = row['recovery_time_factor']
                    metric_data['total_performance_factor'] = row['total_performance_factor']
                    metric_data['absorption_factor'] = row['absorption_factor']
                    metric_data['recovery_factor'] = row['recovery_factor']
					# TODO record observer time
                    metric_data['observer_time'] = 120
                    break
            if 'rpo' not in metric_data:
                continue
            metric_datas.append(metric_data)
        # sort metric_datas by fault_name
        metric_datas = sorted(metric_datas, key=lambda x: x['fault_name'])
        return metric_datas        

    def delete_result(self, run_id):
        try:
            run_id = int(run_id)
        except Exception as e:
            return "What?"

        self.lock.acquire()
        html_path = os.path.join(self.data_dir, "result_{0:06d}.html".format(run_id))
        data_path = os.path.join(self.data_dir, "result_{0:06d}".format(run_id))
        new_results = [x for x in self.status_data['results'] if x['run_id'] != run_id]
        if len(new_results) > 0:
            new_count = max([x['run_id'] for x in new_results])
        else:
            new_count = 0

        try:
            shutil.rmtree(data_path)
        except Exception as e:
            print(str(e))
        try:
            os.remove(html_path)
        except Exception as e:
            print(str(e))
        self.status_data['run_count'] = new_count
        self.status_data['results'] = new_results

        self.save_status()
        with open(os.path.join(self.data_dir, 'run_seq.dat'), 'w') as fd:
            fd.write(str(new_count) + '\n')

        self.lock.release()

    def run_benchmark(self):
        self.lock.acquire()
        if self.current_job_type != 'IDLE':
            self.lock.release()
            return False

        self.status_data['run_count'] += 1
        run_id = self.status_data['run_count']
        self.status_data['results'] = [
            {
                'run_id':   run_id,
                'name':     "result_{0:06d}".format(run_id),
                'start':    time.asctime(),
                'state':    'RUN',
            }] + self.status_data['results']
        self.save_status()

        self.current_job_type = 'RUN'
        self.current_job_id = run_id
        self.current_job_name = "result_{0:06d}".format(run_id)
        self.current_job = RunBenchmark(self, run_id)
        self.current_job_output = ""
        self.current_job_start = time.time()
        self.current_job.start()
        self.lock.release()
        return True

    def run_allfaults(self):
        self.lock.acquire()
        if self.current_job_type != 'IDLE':
            self.lock.release()
            return

        run_id = self.status_data['run_count'] + 1
        self.status_data['run_count'] += 1
        self.status_data['results'] = [
            {
                'run_id':   run_id,
                'name':     "result_{0:06d}".format(run_id),
                'start':    time.asctime(),
                'state':    'ALLRUNNING',
				        'start_run_id': run_id+1,
				        'end_run_id': 0,
            }] + self.status_data['results']
        self.save_status()

        self.current_job = RunAllFaults(self, run_id)
        self.current_job_output = ""
        self.current_job_start = time.time()
        self.current_job.start()
        self.lock.release()
        
    def run_build(self):
        self.lock.acquire()
        if self.current_job_type != 'IDLE':
            self.lock.release()
            return

        self.current_job_type = 'BUILD'
        self.current_job = RunDatabaseBuild(self)
        self.current_job_output = ""
        self.current_job_start = time.time()
        self.current_job.start()
        self.lock.release()

    def run_destroy(self):
        self.lock.acquire()
        if self.current_job_type != 'IDLE':
            self.lock.release()
            return

        self.current_job_type = 'DESTROY'
        self.current_job = RunDatabaseDestroy(self)
        self.current_job_output = ""
        self.current_job_start = time.time()
        self.current_job.start()
        self.lock.release()

    def cancel_job(self):
        self.lock.acquire()
        if self.current_job is None:
            print("no current job")
            self.lock.release()
            return
        if self.current_job.proc is None:
            print("current job has no process")
            self.lock.release()
            return
        for entry in self.status_data['results']:
            if entry['name'] == self.current_job_name:
                entry['state'] = 'CANCELED'
                break
        os.killpg(os.getpgid(self.current_job.proc.pid), signal.SIGKILL)
        self.finished_reson = "CANCELED"
        self.save_status()
        self.lock.release()

class RunBenchmark(threading.Thread):
    def __init__(self, bench, run_id):
        threading.Thread.__init__(self)

        self.bench = bench
        self.run_id = run_id
        self.proc = None

    def run(self):
        last_props = os.path.join(self.bench.data_dir, 'last.properties')
        run_props = os.path.join(self.bench.data_dir, 'run.properties')
        result_dir = os.path.join(self.bench.data_dir, "result_{0:06d}".format(self.run_id))

        with open(last_props, 'r') as fd:
            props = fd.read()
        with open(run_props, 'w') as fd:
            fd.write(props)
            fd.write("\n")
            fd.write("resultDirectory={0}\n".format(result_dir))
        with open(os.path.join(self.bench.data_dir, 'run_seq.dat'), 'w') as fd:
            fd.write(str(self.run_id - 1) + '\n')
          
        # init sqlite connection
        self.db_conn = sqlite3.connect(os.path.join(self.bench.data_dir, 'benchmark.db'))
        # create a table to store batch_id and run_id
        cursor = self.db_conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS batch_runs (
                run_id INTEGER PRIMARY KEY,
                batch_id INTEGER
                )''')
        # store batch_id and  run_id into batch_runs
        cursor.execute('INSERT INTO batch_runs (run_id, batch_id) VALUES (?, ?)', (self.run_id, self.run_id))
        # create table metric, columns are run_id(int), data_loss_seconds(float), interrupt_time_seconds(float), stablity(float), dbtype(string), fault_type(string), scope(string), role(string), num(int)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metrics (
                run_id INTEGER PRIMARY KEY,
                data_loss_seconds REAL,
                interrupt_time_seconds REAL,
                stability REAL,
                dbtype TEXT,
                fault_type TEXT,
                scope TEXT,
                role TEXT,
                num INTEGER
            )''')   
        self.db_conn.commit()

        cmd = ['./runBenchmark.sh', run_props, ]
        self.proc = subprocess.Popen(cmd,
                                     stdout = subprocess.PIPE,
                                     stderr = subprocess.STDOUT,
                                     stdin = None,
                                     preexec_fn = os.setsid)
        while True:
            line = self.proc.stdout.readline().decode('utf-8')
            if len(line) == 0:
                break
            self.bench.add_job_output(line)
        self.proc.wait()
        rc = self.proc.returncode
        self.proc = None

        if rc != 0:
            self.bench.add_job_output("\n\nBenchmarkSQL had exit code {0}\n".format(rc))
			# set current job state to RUNFAILED
            self.bench.lock.acquire()
            for entry in self.bench.status_data['results']:
                if entry['name'] == self.bench.current_job_name:
                    entry['state'] = 'RUNFAILED'
                    break
            self.bench.save_status()
            self.bench.lock.release()

        # ----
        # Read the current run properties and parse them this time.
        # We need to get the reportScript= property from that.
        # ----
        jprop = jproperties.Properties()
        with open(run_props, "rb") as fd:
            jprop.load(fd, 'utf-8')
        if 'reportScript' in jprop:

            self.bench.add_job_output("\nBenchmarkSQL run complete - generating report\n")
            cmd = shlex.split(jprop['reportScript'].data)
            cmd.append('--resultdir')
            cmd.append(result_dir)
            self.proc = subprocess.Popen(cmd,
                                         stdout = subprocess.PIPE,
                                         stderr = subprocess.STDOUT,
                                         stdin = None,
                                         preexec_fn = os.setsid)
            while True:
                line = self.proc.stdout.readline().decode('utf-8')
                if len(line) == 0:
                    break
                self.bench.add_job_output(line)
            self.proc.wait()
            rc = self.proc.returncode
            self.proc = None

            if rc != 0:
                self.bench.add_job_output("\n\nreportScript had exit code {0} - report may be incomplete\n".format(rc))

        else:
            self.bench.add_job_output("\nBenchmarkSQL run complete\n")

        # create result dir if not exists
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)
        result_log = os.path.join(result_dir, 'console.log')
        with codecs.open(result_log, 'w', encoding='utf8') as fd:
            fd.write(self.bench.current_job_output)

class RunAllFaults(threading.Thread):
    def __init__(self, bench, run_id):
        threading.Thread.__init__(self)

        self.bench = bench
        self.batch_id = run_id
        self.run_id = run_id + 1
        self.start_run_id = run_id + 1
        self.end_run_id = 0
        self.proc = None
                

    def run(self):
        self.bench.current_job_type = 'RUNALL'
        self.bench.current_job_id = self.batch_id 
        self.bench.current_job_name = "result_{0:06d}".format(self.batch_id)
        self.bench.save_status()

                # init sqlite connection
        self.db_conn = sqlite3.connect(os.path.join(self.bench.data_dir, 'benchmark.db'))
        # create a table to store batch_id and run_id
        cursor = self.db_conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS batch_runs (
                run_id INTEGER PRIMARY KEY,
                batch_id INTEGER
                )''')
        # create table metric, columns are run_id(int), data_loss_seconds(float), interrupt_time_seconds(float), stablity(float), dbtype(string), fault_type(string), scope(string), role(string), num(int)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metrics (
                run_id INTEGER PRIMARY KEY,
                data_loss_seconds REAL,
                interrupt_time_seconds REAL,
                stablity REAL,
                dbtype TEXT,
                fault_type TEXT,
                scope TEXT,
                role TEXT,
                num INTEGER
            )''')   
        self.db_conn.close()
  
        fault_files = [f for f in os.listdir(self.bench.faults_dis) if os.path.isfile(os.path.join(self.bench.faults_dis, f))]
        for fault_file in fault_files:
            if self.bench.finished_reson == "CANCELED":
                self.bench.finished_reson = ""
                return 
            if self.run_each(fault_file) is True:
                self.bench.status_data['run_count'] += 1
                self.run_id += 1
            	# sleep 30 seconds
                time.sleep(60)
            # clear job output
            self.bench.current_job_output = ""
        self.end_run_id = self.run_id
        rpos, rtos = [], []
        # read metrics from result_dir and compute average result of metrics
        for run_id in range(self.start_run_id, self.end_run_id):
            result_dir = os.path.join(self.bench.data_dir, "result_{0:06d}".format(run_id))
            # read rto and rpo from result_dir/data/metrics.csv
            with open(os.path.join(result_dir, 'data', 'metrics.csv'), 'r') as fd:
                csv_reader = csv.DictReader(fd)
                for row in csv_reader:
                    rpos.append(float(row['rpo']))
                    rtos.append(float(row['rto']))

        # compute average rpo and rto
        avg_rpo = sum(rpos) / (self.end_run_id - self.start_run_id)
        avg_rto = sum(rtos) / (self.end_run_id - self.start_run_id)
        line = "Average RPO: {0}, Average RTO: {1}\n".format(avg_rpo, avg_rto)
        self.bench.add_job_output(line)
        # change current job state to ALLFINISHED
        self.bench.lock.acquire()
        for entry in self.bench.status_data['results']:
            if entry['run_id'] == self.batch_id:
                entry['state'] = 'ALLFINISHED'
                entry['start_run_id'] = self.start_run_id
                entry['end_run_id'] = self.end_run_id
                break
            if entry['name'] == self.bench.current_job_name:
                entry['state'] = 'ALLFINISHED'
                entry['start_run_id'] = self.start_run_id
                entry['end_run_id'] = self.end_run_id
                break
        self.bench.current_job_type = 'IDLE'
        self.bench.current_job_id = 0
        self.bench.current_job_name = ""
        self.bench.current_sub_job_name = ""
        self.bench.current_job = None
        self.bench.save_status()
        self.bench.lock.release()

    def run_each(self, fault_file):
        last_props = os.path.join(self.bench.data_dir, 'last.properties')
        run_props = os.path.join(self.bench.data_dir, 'run.properties')
        result_dir = os.path.join(self.bench.data_dir, "result_{0:06d}".format(self.run_id))
        
        # store batch_id and  run_id into batch_runs
        cursor = self.db_conn.cursor()
        cursor.execute('INSERT INTO batch_runs (run_id, batch_id) VALUES (?, ?)', (self.run_id, self.batch_id))
        self.db_conn.commit()

        # clear result_dir if exists
        if os.path.exists(result_dir):
            shutil.rmtree(result_dir)
        # os.makedirs(result_dir)

        # update bench.status_data['results']
        self.bench.status_data['results'] = [{
                'run_id':   self.run_id,
                'name':     "result_{0:06d}".format(self.run_id),
                'start':    time.asctime(),
                'state':    'RUN',
            }] + self.bench.status_data['results']
        self.bench.save_status()
        # self.bench.current_job_type = 'RUNALL'
        # self.bench.current_job_id = self.run_id
        self.bench.current_sub_job_name = "result_{0:06d}".format(self.run_id)
        # self.bench.save_status()

        origin_props = jproperties.Properties()
        with open(last_props, 'rb') as fd:
            origin_props.load(fd, 'utf-8')
        
        # set sys.faults = $fault_file, and resultDirectory = $result_dir and scenario=$fault_file
        origin_props['sys.faults'] = fault_file
        origin_props['resultDirectory'] = result_dir
        origin_props['scenario'] = fault_file
        with open(run_props, 'wb') as fd:
            origin_props.store(fd, 'utf-8')
        # clear and store last properties
        with open(last_props, 'wb') as fd:
            origin_props.store(fd, 'utf-8')

        with open(os.path.join(self.bench.data_dir, 'run_seq.dat'), 'w') as fd:
            fd.write(str(self.run_id - 1) + '\n')

        cmd = ['./runBenchmark.sh', run_props, ]
        self.proc = subprocess.Popen(cmd,
                                     stdout = subprocess.PIPE,
                                     stderr = subprocess.STDOUT,
                                     stdin = None,
                                     preexec_fn = os.setsid)
        while True:
            line = self.proc.stdout.readline().decode('utf-8')
            if len(line) == 0:
                break
            self.bench.add_job_output(line)
        self.proc.wait()
        rc = self.proc.returncode
        self.proc = None

        if rc != 0:
            self.bench.add_job_output("\n\nBenchmarkSQL had exit code {0}\n".format(rc))
            # RUNFAILED, just skip current case
            # remove current status_data and exit
            self.bench.lock.acquire()
            for entry in self.bench.status_data['results']:
                if entry['name'] == self.bench.current_sub_job_name:
                    # remove entry from status_data['results']
                    self.bench.status_data['results'].remove(entry)
                    break
            self.bench.save_status()
            # clear job_output

            self.bench.current_job_output = ""
            self.bench.lock.release()
            return False

        # ----
        # Read the current run properties and parse them this time.
        # We need to get the reportScript= property from that.
        # ----
        jprop = jproperties.Properties()
        with open(run_props, "rb") as fd:
            jprop.load(fd, 'utf-8')
        if 'reportScript' in jprop:

            self.bench.add_job_output("\nBenchmarkSQL run complete - generating report\n")
            cmd = shlex.split(jprop['reportScript'].data)
            cmd.append('--resultdir')
            cmd.append(result_dir)
            self.proc = subprocess.Popen(cmd,
                                         stdout = subprocess.PIPE,
                                         stderr = subprocess.STDOUT,
                                         stdin = None,
                                         preexec_fn = os.setsid)
            while True:
                line = self.proc.stdout.readline().decode('utf-8')
                if len(line) == 0:
                    break
                self.bench.add_job_output(line)
            self.proc.wait()
            rc = self.proc.returncode
            self.proc = None

            if rc != 0:
                self.bench.add_job_output("\n\nreportScript had exit code {0} - report may be incomplete\n".format(rc))

        else:
            self.bench.add_job_output("\nBenchmarkSQL run complete\n")
        
        for entry in self.bench.status_data['results']:
            if entry['name'] == self.bench.current_sub_job_name:
                if entry['state'] == 'RUN':
                    entry['state'] = 'FINISHED'
                break
        # self.bench.current_job = None
        # self.bench.current_job_id = 0
        # self.bench.current_job_type = 'IDLE'
        # self.bench.current_job_name = ""
        self.bench.save_status()

        if not os.path.exists(result_dir):
            os.makedirs(result_dir)
        result_log = os.path.join(result_dir, 'console.log')
        with codecs.open(result_log, 'w', encoding='utf8') as fd:
            fd.write(self.bench.current_job_output)
        return True

class RunDatabaseBuild(threading.Thread):
    def __init__(self, bench):
        threading.Thread.__init__(self)

        self.bench = bench
        self.proc = None

    def run(self):
        last_props = os.path.join(self.bench.data_dir, 'last.properties')
        run_props = os.path.join(self.bench.data_dir, 'run.properties')

        with open(last_props, 'r') as fd:
            props = fd.read()
        with open(run_props, 'w') as fd:
            fd.write(props)

        cmd = ['./runDatabaseBuild.sh', run_props, ]
        self.proc = subprocess.Popen(cmd,
                                     stdout = subprocess.PIPE,
                                     stderr = subprocess.STDOUT,
                                     stdin = None,
                                     preexec_fn = os.setsid)
        while True:
            line = self.proc.stdout.readline().decode('utf-8')
            if len(line) == 0:
                break
            self.bench.add_job_output(line)
        self.proc.wait()
        rc = self.proc.returncode
        self.proc = None

        if rc != 0:
            self.bench.add_job_output("\n\nBenchmarkSQL terminated with exit code {0}\n".format(rc))
        return

class RunDatabaseDestroy(threading.Thread):
    def __init__(self, bench):
        threading.Thread.__init__(self)

        self.bench = bench
        self.proc = None

    def run(self):
        last_props = os.path.join(self.bench.data_dir, 'last.properties')
        run_props = os.path.join(self.bench.data_dir, 'run.properties')

        with open(last_props, 'r') as fd:
            props = fd.read()
        with open(run_props, 'w') as fd:
            fd.write(props)

        cmd = ['./runDatabaseDestroy.sh', run_props, ]
        self.proc = subprocess.Popen(cmd,
                                     stdout = subprocess.PIPE,
                                     stderr = subprocess.STDOUT,
                                     stdin = None,
                                     preexec_fn = os.setsid)
        while True:
            line = self.proc.stdout.readline().decode('utf-8')
            if len(line) == 0:
                break
            self.bench.add_job_output(line)
        self.proc.wait()
        rc = self.proc.returncode
        self.proc = None

        if rc != 0:
            self.bench.add_job_output("\n\nBenchmarkSQL terminated with exit code {0}\n".format(rc))
        return
