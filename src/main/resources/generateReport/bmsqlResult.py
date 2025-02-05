import os.path
import csv
import math
import json
import re
import datetime
import duckdb
import pymser
import yaml
from scipy.signal import savgol_filter

class bmsqlResult:
    def __init__(self, resdir):
        """
        Create a new bmsqlResult instance and load all the data
        in the result directory.
        """
        self.ttypes = [
                'NEW_ORDER',
                'PAYMENT',
                'ORDER_STATUS',
                'STOCK_LEVEL',
                'DELIVERY',
                'DELIVERY_BG',
				'STORE',
            ]
        self.resdir = resdir
        self.datadir = os.path.join(resdir, 'data')

        # ----
        # Load the run info into a dict
        # ----
        fname = os.path.join(self.datadir, 'runInfo.csv')
        with open(fname, newline = '') as fd:
            rdr = csv.DictReader(fd)
            self.runinfo = next(rdr)

        self.faultinfo = {}
        fault_fname = os.path.join(self.datadir, 'faultInfo.csv')
        with open(fault_fname, newline = '') as fd:
            rdr = csv.DictReader(fd)
            # if there is no fault, the faultinfo will be empty even
            for row in rdr:
                # only one fault is allowed currently
                self.faultinfo = dict(row)
                break
		
        self.faultfile = None
        self.faultfilename = os.path.join(self.datadir, 'fault.yaml')
        if os.path.exists(self.faultfilename):
            with open(self.faultfilename, 'r') as fd:
                # load the fault file using yaml
                self.faultfile = yaml.safe_load(fd.read())

        self.trace_fname = os.path.join(self.datadir, 'trace.csv')
        with open(self.trace_fname, newline='') as fd:
            rdr = csv.DictReader(fd)
            self.txn_trace = [row for row in rdr]

        txn_fname = os.path.join(self.datadir, 'txnlog.csv')

        self.rto = self.rto(self.trace_fname)
        self.rpo = self.rpo(self.trace_fname, txn_fname)
        self.steady_metrics = self.steady_metrics()

        # write rto and rpo result into metrics.csv
        metrics_fname = os.path.join(self.datadir, 'metrics.csv')
        with open(metrics_fname, 'w', newline='') as fd:
            wrt = csv.writer(fd)
            wrt.writerow(['rto', 'rpo', 'recovery_time_factor', 'total_performance_factor', 'absorption_factor', 'recovery_factor'])
            wrt.writerow([self.rto, self.rpo, self.steady_metrics[-1]['recovery_time_factor'], self.steady_metrics[-1]['total_performance_factor'], self.steady_metrics[-1]['absorption_factor'], self.steady_metrics[-1]['recovery_factor']])

        # self.stage_latency()
        # self.stage_throughput()

        # ----
        # Load the other CSV files into dicts of arrays.
        #
        #   result_ttype    a dict of result_data slices by transaction type
        #
        #   summary_ttype   a dict of transaction summary info per type
        #
        #   hist_ttype      a dict of hist_data slices by transaction type
        #
        #   hist_bins       the number of bins in the histogram
        #
        #   hist_cutoff     the edge of the last bin in the histogram
        #
        # Loading of the summary will fail if the benchmark run is
        # still in progress or has been aborted. We then return with
        # an incoplete result, which still allows drawing graphs.
        # ----
        self.result_ttype = self._load_ttype_csv_multiple('result.csv')
        try:
            self.summary_ttype = self._load_ttype_csv_single('summary.csv')
        except StopIteration:
            return
        self.hist_ttype = self._load_ttype_csv_multiple('histogram.csv')
        self.hist_bins = len(self.hist_ttype['NEW_ORDER'])
        self.hist_cutoff = self.hist_ttype['NEW_ORDER'][-1][0]
        self.hist_statsdiv = math.log(self.hist_cutoff * 1000.0) / self.hist_bins

        # ----
        # The total number of "measured" transactions is the sum summary
        # counts but without the delivery background transactions.
        # ----
        self.total_trans = (sum([self.summary_ttype[tt][0]
                                for tt in self.ttypes])
                                - self.summary_ttype['DELIVERY_BG'][0])

        # ----
        # If an OS metric collector was running, load its data.
        # ----
        os_metric_fname = os.path.join(self.datadir, 'os-metric.json')
        if os.path.exists(os_metric_fname):
            with open(os_metric_fname) as fd:
                self.os_metric = json.loads(fd.read())
        else:
            self.os_metric = {}

        # ----
        # Load the run.properties but remove the password
        # ----
        prop_fname = os.path.join(resdir, 'run.properties')
        with open(prop_fname, 'r') as fd:
            props = fd.read()
        # parse the properties file, and find scenario name, if no scenario name, set it to uncertain
        self.scenario_name = re.search(r'scenario\s*=\s*(.*)', props, re.M).group(1) if re.search(r'scenario\s*=\s*(.*)', props, re.M) else "uncertain"
        self.properties = re.sub(r'(password\s*=\s*).*$', r'\1********',
                                 props, flags = re.M)

    def tpm_c(self):
        num_new_order = self.summary_ttype['NEW_ORDER'][0]
        return num_new_order / int(self.runinfo['runMins'])

    def tpm_total(self):
        return self.total_trans / int(self.runinfo['runMins'])

    def percentile(self, tt, nth):
        """
        Returns the nth percentile response time of transaction type tt
        """
        nth_have = 0
        nth_need = int(self.summary_ttype[tt][0] * nth)
        b = 0
        for b in range(0, self.hist_bins):
            if nth_have >= nth_need:
                break
            nth_have += int(self.hist_ttype[tt][b][1])
        return math.exp(float(b) * self.hist_statsdiv) / 1000.0

    def num_trans(self, tt):
        """
        Returns the total number of transaction for the given type
        during the measurement cycle
        """
        return int(self.summary_ttype[tt][0])

    def trans_mix(self, tt):
        """
        Returns the percentage of the transaction type overall
        """
        return self.summary_ttype[tt][1]

    def avg_latency(self, tt):
        """
        Returns the average latency for the given transaction type
        during the measurement cycle
        """
        return self.summary_ttype[tt][2]

    def max_latency(self, tt):
        """
        Returns the maximum latency for the given transaction type
        during the measurement cycle
        """
        return self.summary_ttype[tt][3]

    def num_rollbacks(self, tt):
        """
        Returns the number of rollbacks that happened for the transaction
        type. This is only useful for NEW_ORDER.
        """
        return int(self.summary_ttype[tt][4])

    def num_errors(self, tt):
        """
        Returns the number of errors encountered for the transaction type
        during the measurement cycle
        """
        return int(self.summary_ttype[tt][5])

    def _load_ttype_csv_single(self, fname, skip_header = True):
        """
        Read a CSV file that has the transaction type as the first element.
        We expect a single row per transaction type.
        """
        ttdict = {}
        path = os.path.join(self.datadir, fname)
        with open(path, newline = '') as fd:
            rdr = csv.reader(fd)
            if skip_header:
                _ = next(rdr)
            for row in rdr:
                tt = row[0]
                ttdict[tt] = [float(d) for d in row[1:]]

        return ttdict

    def _load_ttype_csv_multiple(self, fname, skip_header = True):
        """
        Read a CSV file that has the transaction type as the first element.
        Return a list of tuples as well as a dict that has lists of tuples
        separated by transaction type.
        """
        ttdict = {}
        path = os.path.join(self.datadir, fname)
        with open(path, newline = '') as fd:
            rdr = csv.reader(fd)
            if skip_header:
                _ = next(rdr)
            data = [[row[0], [float(d) for d in row[1:]]] for row in rdr]

            for ttype in self.ttypes:
                tuples = filter(lambda x : x[0] == ttype, data)
                ttdict[ttype] = [tup[1] for tup in tuples]

        return ttdict

    # def stage_latency(self):
    #    """
    #    Print the average latency for different stage, including last minute before the fault, duration time of fault, and average latency for first minute after the fault
    #    """
    #    if self.faultinfo == {}:
    #         print("No fault found")
    #         return
    #    fault_time = int(self.faultinfo["start"])
    #    before_fault = (fault_time - 60000, fault_time)
    #    fault_duration = (fault_time, fault_time + int(self.faultinfo["duration"]))
    #    after_fault = (fault_time + int(self.faultinfo["duration"]), fault_time + int(self.faultinfo["duration"]) + 60000)
    #    before_fault_latency = []
    #    fault_duration_latency = []
    #    after_fault_latency = []
    #    for row in self.txn_trace:
    #        if before_fault[0] <= int(row["start"]) < before_fault[1]:
    #            before_fault_latency.append(int(row["end"]) - int(row["start"]))
    #        elif fault_duration[0] <= int(row["start"]) < fault_duration[1]:
    #            fault_duration_latency.append(int(row["end"]) - int(row["start"]))
    #        elif after_fault[0] <= int(row["start"]) < after_fault[1]:
    #            after_fault_latency.append(int(row["end"]) - int(row["start"]))
    #    print("Average latency before fault: ", sum(before_fault_latency) / len(before_fault_latency) , " ms")
    #    if len(fault_duration_latency) > 0:
    #         # compute the average latency during fault and change rate of latency during fault
    #         print("Average latency during fault: ", sum(fault_duration_latency) / len(fault_duration_latency) , " ms",  " change rate: ", (sum(fault_duration_latency) / len(fault_duration_latency) - sum(before_fault_latency) / len(before_fault_latency)) / (sum(before_fault_latency) / len(before_fault_latency)) * 100, " %")
    #    print("Average latency after fault: ", sum(after_fault_latency) / len(after_fault_latency) , " ms", " change rate: ", (sum(after_fault_latency) / len(after_fault_latency) - sum(before_fault_latency) / len(before_fault_latency)) / (sum(before_fault_latency) / len(before_fault_latency)) * 100, " %")

    # def stage_throughput(self):
    #     """
    #     Print the average throughput for different stage, including last minute before the fault, duration time of fault, and average throughput for first minute after the fault
    #     """
    #     if self.faultinfo == {}:
    #         print("No fault found")
    #         return
    #     fault_time = int(self.faultinfo["start"])
    #     before_fault = (fault_time - 60000, fault_time)
    #     fault_duration = (fault_time, fault_time + int(self.faultinfo["duration"]))
    #     after_fault = (fault_time + int(self.faultinfo["duration"]), fault_time + int(self.faultinfo["duration"]) + 60000)
    #     before_fault_throughput = 0
    #     fault_duration_throughput = 0
    #     after_fault_throughput = 0
    #     for row in self.txn_trace:
    #         if before_fault[0] <= int(row["start"]) < before_fault[1]:
    #             before_fault_throughput += 1
    #         elif fault_duration[0] <= int(row["start"]) < fault_duration[1]:
    #             fault_duration_throughput += 1
    #         elif after_fault[0] <= int(row["start"]) < after_fault[1]:
    #             after_fault_throughput += 1
    #     print("Average throughput before fault: ", before_fault_throughput / 60 , " txn/s")
    #     if int(self.faultinfo["duration"]) > 0:
    #         print("Average throughput during fault: ", 1000 * fault_duration_throughput / int(self.faultinfo["duration"]), " txn/s", " change rate: ", ((1000 * fault_duration_throughput / int(self.faultinfo["duration"])) - (before_fault_throughput / 60)) / (before_fault_throughput / 60) * 100, " %")
    #     print("Average throughput after fault: ", after_fault_throughput / 60 , " txn/s", " change rate: ", ((after_fault_throughput / 60) - (before_fault_throughput / 60)) / (before_fault_throughput / 60) * 100, " %")

    def rto(self, trace_file):
        """
        Returns the recovery time objective in seconds.
        """
        if self.faultinfo == {}:
            print("No fault found")
            return -1

        con = duckdb.connect()

        # create a table from the trace file
		# CREATE TABLE transactions AS SELECT * FROM read_csv_auto('service_data/result_000162/data/trace.csv');
        con.execute("""
            CREATE TABLE transactions AS SELECT * FROM read_csv_auto('""" + trace_file + """');""")

		# get threads number
        threads_num = con.execute("SELECT COUNT(DISTINCT (txn_id >> 32)) FROM transactions;").fetchone()[0]

        intervals_per_thread = []

        # compute average latency of successful transactions before fault but after warmup
        runinfo = self.runinfo
        start_ts = int(runinfo["startTS"])
        warmup_ts = start_ts + int(runinfo["rampupMins"]) * 60 * 1000
        fault_start_ts = int(self.faultinfo["start"])
       
        # compute p95 latency of successful transactions before fault but after warmup 
        p95 = con.execute(""" 
            SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY (transactions .end - transactions.start)) FROM transactions WHERE error = 0 AND rollback = 0 AND transactions.end > """ + str(warmup_ts) + """ AND transactions.end < """ + str(fault_start_ts) + """;
        """).fetchone()[0]
        con.close()

        trace_pre_thread = {}
        for row in self.txn_trace:
            thread_id = int(row["txn_id"]) >> 32
            if thread_id not in trace_pre_thread:
                trace_pre_thread[thread_id] = []
            if row['error'] == '1' or row['rollback'] == '1':
                continue
            trace_pre_thread[thread_id].append(row)
    
        intervals_per_thread = {}
        for thread_id in trace_pre_thread:
            intervals_per_thread[thread_id] = []
            txn_trace = trace_pre_thread[thread_id]
            sorted_txn_trace = sorted(txn_trace, key=lambda x: int(x["end"]))

            # select a interrupt interval, which is the transaction gap there is no successful transaction after it for 5*average_latency
            for (row, row_next) in zip(sorted_txn_trace[:-1], sorted_txn_trace[1:]):
                if int(row_next['end']) > warmup_ts and int(row_next['end']) - int(row['end']) > max(5 * p95, 2000):
                # if int(row_next['end']) > warmup_ts and int(row_next['end']) - int(row['end']) > 5 * p95:
                    intervals_per_thread[thread_id].append((int(row['end']), int(row_next['end'])))

        # compute sum of interval duration for each thread_id, and compute the average interrupt time according thread_id
        interrupt_time = {}
        for thread_id, intervals in intervals_per_thread.items():
            interrupt_time[thread_id] = sum([interval[1] - interval[0] for interval in intervals])

        avg_interrupt_time = sum(interrupt_time.values()) / int(threads_num)

        # print all interrupt intervals
        for thread_id, intervals in intervals_per_thread.items():
            for interval in intervals:
                print(f"Thread {thread_id:03d}: interrupt start time: {datetime.datetime.fromtimestamp(interval[0] / 1000)}, recovery end time: {datetime.datetime.fromtimestamp(interval[1] / 1000)}, recovery time: {interval[1] - interval[0]} ms")

        print(f"Average interrupt time: {avg_interrupt_time} ms")

        # compute the standard deviation of interrupt time
        std_interrupt_time = math.sqrt(sum([(t - avg_interrupt_time) ** 2 for t in interrupt_time.values()]) / int(threads_num))
        print(f"Standard deviation of interrupt time: {std_interrupt_time} ms")

        return avg_interrupt_time

    def rpo(self, trace_file, txn_file):
        """
        Returns the recovery point objective in milliseconds.
        """
        if self.faultinfo == {}:
            print("No fault found")
            return -1

        # get all txn from trace file, and compare with txn file
        # find all loss txn_id who are in trace file but are not in txn file
        # the rpo result is the merge interval of all loss txn_id interval
        # for example, if the loss txn_id interval(start, end) is [1, 3], [2, 6], [5, 9], [15, 17], then the rpo result is len([1, 9]) + len([15, 17]) = 8 + 2 = 10

        persisted_txn_ids = set()
        with open(txn_file, newline='') as fd:
            rdr = csv.DictReader(fd)
            for row in rdr:
                persisted_txn_ids.add(row['txn_id'])

        loss_txn_intervals = []
        with open(trace_file, newline='') as fd:
            rdr = csv.DictReader(fd)
            for row in rdr:
                if int(row['error'])==1 or int(row['rollback'])==1:
                    continue
                if row['txn_id'] not in persisted_txn_ids:
                    loss_txn_intervals.append((int(row['start']), int(row['end'])))

        loss_txn_intervals.sort()
        merged_intervals = []
        for interval in loss_txn_intervals:
            if not merged_intervals or merged_intervals[-1][1] < interval[0]:
                merged_intervals.append(interval)
            else:
                merged_intervals[-1] = (merged_intervals[-1][0], max(merged_intervals[-1][1], interval[1]))

        rpo = sum([interval[1] - interval[0] for interval in merged_intervals])

        print(f"RPO: {rpo} ms")
        return rpo

    def parse_duration(self, duration):
        """
        将时间字符串（如 '480s', '2m', '100ms', '1.5h'）转换为秒数（浮点数）。
        """
        if duration.endswith('ms'):
            return int(duration[:-2]) / 1000  # 毫秒转秒
        elif duration.endswith('s'):
            return int(duration[:-1])  # 秒
        elif duration.endswith('m'):
            return int(duration[:-1]) * 60  # 分钟转秒
        elif duration.endswith('h'):
            return int(duration[:-1]) * 3600  # 小时转秒
        else:
            raise ValueError(f"未知的时间格式: {duration}")

    # 递归解析模板
    def parse_template(self, template_name, templates, start_time=0):
        # 找到当前模板
        template = next(t for t in templates if t['name'] == template_name)
        result = []

        # 如果是 Serial 模板
        if template['templateType'] == 'Serial':
            current_time = start_time
            for child in template.get('children', []):
                child_results = self.parse_template(child, templates, current_time)
                result.extend(child_results)
                if child_results:
                    current_time = child_results[-1]['end_time']  # 更新当前时间

        # 如果是 Parallel 模板
        elif template['templateType'] == 'Parallel':
            for child in template.get('children', []):
                child_results = self.parse_template(child, templates, start_time)
                result.extend(child_results)

        # 如果是具体的 Chaos 模板（如 NetworkChaos 或 PodChaos）
        else:
            if 'deadline' in template:
                if template['templateType'] == 'PodChaos' and template['podChaos']['action'] == 'pod-kill':
                    duration = 0  # pod-kill 的 duration 为 0
                else:
                    duration = self.parse_duration(template['deadline'])
                result.append({
                    'name': template['name'],
                    'templateType': template['templateType'],
                    'start_time': start_time,
                    'end_time': start_time + duration,
                    'duration_seconds': duration
                })

        return result

    def fault_seconds(self):
        """
        Returns a tuple of fault start time and fault end time in seconds
        """
        runinfo = self.runinfo
        startTS = (int(runinfo['startTS']))
        total_seconds = int(runinfo['rampupMins'])*60 + int(runinfo['runMins'])*60
        rampup_seconds = int(runinfo['rampupMins'])*60
        first_fault_seconds = (int(self.faultinfo['start']) - startTS)//1000
        fault_seconds = []

        fault_file = self.faultfile 
        # parse the chaosmesh yaml file to get all fault start time
        # 1、 read the yaml file （already done）
        # 2、 check spec.action is empty or not
        # 3、 if not empty, only one fault, just use first fault start time
        # 4、 if empty, check spec.entry, and parse the fault workflow recursively, compute the fault time according to serial/parallel and deadline(duration)
        
        if 'action' in fault_file['spec']:
            fault_seconds.append(first_fault_seconds)
        else :
            # parse the fault workflow
            fault_seconds = self.parse_template(fault_file['spec']['entry'], fault_file['spec']['templates'], first_fault_seconds)
            # sort fault_seconds by start_time
            fault_seconds.sort(key=lambda x: x['start_time'])
            # get all fault start time
            fault_seconds = [x['start_time'] for x in fault_seconds]
            # remove duplicate
            fault_seconds = list(set(fault_seconds))
            # sort
            fault_seconds.sort()
        return fault_seconds

    def steady_metric(self, fault_start, period_end, txn_stat, performance_desired):
        rampup_seconds = int(self.runinfo['rampupMins'])*60
        steady_metric = {
            'start_time': fault_start,
            'end_time': period_end,
        }

        # ----
        # Use pymser to find the steady state
        # ----
        txn_stat = savgol_filter(txn_stat, 10, 2)
        sd = txn_stat[fault_start:period_end]
        steady_result = pymser.equilibrate(txn_stat[fault_start:period_end], LLM=True, batch_size=1, ADF_test=True, uncertainty='uSD', print_results=True)

        mse = steady_result['MSE']
        # mse = mse[:math.floor(0.8*len(mse))]
        # mse = savgol_filter(mse, 10, 1)
        # print("mse: {}", mse)

        # find min mse
        minn = 0
        k = 10
        # 找到第一个是比前后k个都小的点
        for i in range(0, len(mse)):
            is_local_min = True
            for j in range(max(0, i-k), min(len(mse), i+k+1)):
                if mse[j] < mse[i]:
                    is_local_min = False
                    break
            if is_local_min:
                minn = i
                break
            
        print("recovery end {}",minn)

        recovery_time = minn
        steady_metric['recovery_time_factor'] = minn

        if recovery_time != 0:
            total_performance = sum(txn_stat[fault_start:fault_start+recovery_time])/ recovery_time
            total_performance_factor = total_performance / performance_desired
            steady_metric['total_performance_factor'] = total_performance_factor
            absorption_factor = min(txn_stat[fault_start:fault_start+recovery_time]) / performance_desired
            steady_metric['absorption_factor'] = absorption_factor
        else:
            steady_metric['total_performance_factor'] = -1
            steady_metric['absorption_factor'] = -1
        
        # avaerage performance in later 1min or all time if recovery time is less than 1min
        performance_recovery = sum(txn_stat[fault_start+recovery_time: min(fault_start+recovery_time+60, period_end)])/ min(60, period_end - fault_start - recovery_time)
        recovery_factor = performance_recovery / performance_desired
        steady_metric['recovery_factor'] = recovery_factor

        print(f"Steady state metrics: {steady_metric}")
        return steady_metric

    def steady_metrics(self):
        """
        Returns a dictsonary of steady state metrics, containing recovery time factor(s, time to steady state), total performance factor(%, total performance/ desired performance), absorption factor(%, performance degradation/ desired performance), recovery factor(%, recovery performance/ desired performance)
        """
        steady_metrics = []
        if self.faultinfo == {}:
            print("No fault found")
            return steady_metrics

        runinfo = self.runinfo

        #TODO@wy change to precise fault time

        total_seconds = int(runinfo['rampupMins'])*60 + int(runinfo['runMins'])*60
        rampup_seconds = int(runinfo['rampupMins'])*60
        startTS = (int(runinfo['startTS']))
        txn_stat = [0 for i in range(total_seconds)]
        for row in self.txn_trace:
            end = (int(row['end']) - startTS)//1000
            if end >=0 and end < total_seconds:
                txn_stat[end]+=1
        
        first_fault_seconds = (int(self.faultinfo['start']) - startTS)//1000
        if first_fault_seconds <= rampup_seconds:
            performance_desired = txn_stat[rampup_seconds]
        else: 
            performance_desired = sum(txn_stat[rampup_seconds:first_fault_seconds])/ (first_fault_seconds- rampup_seconds)

        fault_seconds = self.fault_seconds()
        print(f"fault_seconds: {fault_seconds}")
        # fault_interval is the interval between two fault start time
        fault_interval = []
        for (s, t) in zip(fault_seconds[:-1], fault_seconds[1:]):
            fault_interval.append((s, t))
        fault_interval.append((fault_seconds[-1], total_seconds))
        
        for (fault_start, period_end) in fault_interval:
            steady_metric = self.steady_metric(fault_start, period_end, txn_stat, performance_desired)
            steady_metrics.append(steady_metric)

        return steady_metrics