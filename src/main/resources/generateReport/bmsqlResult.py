import os.path
import csv
import math
import json
import re
import datetime

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

        self.rto = self.rto()

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

    def rto(self):
        """
        Returns the recovery time objective in seconds.
        """
        if self.faultinfo == {}:
            print("No fault found")
            return 
        fault_time = int(self.faultinfo["end"])
        recovery_time = 0
        for row in self.txn_trace:
            if int(row["start"]) > fault_time and int(row["rollback"]) == 0 and int(row["error"]) == 0:
                if recovery_time == 0:
                    recovery_time = int(row["end"])
                recovery_time = min(recovery_time, int(row["end"]))
        # sort txn_trace by end time and find the longest gap of two successful transactions(by end time)
        sorted_txn_trace = sorted(self.txn_trace, key=lambda x: int(x["end"]))
        max_gap, prev_end = 0, 0
        end1, start2, end2=0, 0, 0
        for (i, row) in enumerate(sorted_txn_trace):
            if int(row["rollback"]) == 0 and int(row["error"]) == 0:
                if prev_end == 0:
                    prev_end = int(row["end"])
                else:
                    if max_gap < int(row["end"]) - prev_end:
                        max_gap = int(row["end"]) - prev_end
                        end1, start2, end2 = prev_end, int(row['start']), int(row["end"])
                    prev_end = int(row["end"])
        print("Recovery time: ", datetime.datetime.fromtimestamp(recovery_time/1000), "Fault time: ", datetime.datetime.fromtimestamp(fault_time/1000), "rto: ", (recovery_time - fault_time), "ms")
        print("Max gap: ", max_gap, "ms", "between", datetime.datetime.fromtimestamp(end1/1000), "and", datetime.datetime.fromtimestamp(end2/1000), "start2", datetime.datetime.fromtimestamp(start2/1000))
        return recovery_time - fault_time