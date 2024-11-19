import os.path
import csv
import math
import json
import re
import datetime
import duckdb

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

        trace_fname = os.path.join(self.datadir, 'trace.csv')
        with open(trace_fname, newline='') as fd:
            rdr = csv.DictReader(fd)
            self.txn_trace = [row for row in rdr]

        self.rto = self.rto(trace_fname)
        self.stage_latency()

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

    def stage_latency(self):
       """
       Print the average latency for different stage, including last minute before the fault, duration time of fault, and average latency for first minute after the fault
       """
       if self.faultinfo == {}:
            print("No fault found")
            return 
       fault_time = int(self.faultinfo["start"])
       before_fault = (fault_time - 60000, fault_time)
       fault_duration = (fault_time, fault_time + int(self.faultinfo["duration"]))
       after_fault = (fault_time + int(self.faultinfo["duration"]), fault_time + int(self.faultinfo["duration"]) + 60000)
       before_fault_latency = []
       fault_duration_latency = []
       after_fault_latency = []
       for row in self.txn_trace:
           if before_fault[0] <= int(row["start"]) < before_fault[1]:
               before_fault_latency.append(int(row["end"]) - int(row["start"]))
           elif fault_duration[0] <= int(row["start"]) < fault_duration[1]:
               fault_duration_latency.append(int(row["end"]) - int(row["start"]))
           elif after_fault[0] <= int(row["start"]) < after_fault[1]:
               after_fault_latency.append(int(row["end"]) - int(row["start"]))
       print("Average latency before fault: ", sum(before_fault_latency) / len(before_fault_latency) , " ms")
       if len(fault_duration_latency) > 0:
           print("Average latency during fault: ", sum(fault_duration_latency) / len(fault_duration_latency) , " ms")
       print("Average latency after fault: ", sum(after_fault_latency) / len(after_fault_latency) , " ms")

    def rto(self, trace_file):
        """
        Returns the recovery time objective in seconds.
        """
        if self.faultinfo == {}:
            print("No fault found")
            return 

        con = duckdb.connect()

        # create a table from the trace file
        con.execute("""
            CREATE TABLE transactions AS SELECT * FROM read_csv_auto('""" + trace_file + """');""")

        # find the fault start time and recovery end time
        # step1: find all transactions whose result is error or rollback and record end time as possible fault start time
        # step2: find the first transaction after the fault start time that is not error or rollback and record end time as possible recovery end time, the fault start time and recovery end time pair is a possible rto interval
        # step3: check possible rto intervals whether there are any successful transactions between fault start time and recovery end time, if there are, then this rto interval is invalid
        # step4: find the longest valid rto interval
        max_rto_query = """
            WITH fault_transactions AS (
                SELECT transactions.end AS fault_start
                FROM transactions
                WHERE error = 1 OR rollback = 1
            ),
            recovery_transactions AS (
                SELECT ft.fault_start AS fault_start, Min(t.end) AS recovery_end
                FROM fault_transactions ft
                JOIN transactions t ON t.end > ft.fault_start
                WHERE t.error = 0 AND t.rollback = 0
                GROUP BY ft.fault_start
            ),
            rto_calculation AS (
                SELECT rt.fault_start, rt.recovery_end,
                    COUNT(CASE WHEN t.start IS NOT NULL AND t.end IS NOT NULL THEN 1 END) OVER (PARTITION BY rt.fault_start) AS success_count
                FROM recovery_transactions rt 
                LEFT JOIN transactions t ON t.end > rt.fault_start AND t.end < rt.recovery_end AND t.error = 0 AND t.rollback = 0
            )
            SELECT MAX(recovery_end - fault_start) AS max_rto
            FROM rto_calculation
            WHERE success_count = 0;
        """

        # get max RTO
        max_rto = con.execute(max_rto_query).fetchone()[0]

        if max_rto:
            print(f"Max RTO: {max_rto} ms")
        else:
            print("No valid recovery stage found.")

        con.close()