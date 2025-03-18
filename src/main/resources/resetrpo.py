import os
import csv
import subprocess

# 定义 service_data 目录的路径
service_data_dir = './service_data'

# 遍历 service_data 目录下的所有子目录
for root, dirs, files in os.walk(service_data_dir):
    for dir_name in dirs:
        # 构建 resultdir 的路径
        resultdir = os.path.join(root, dir_name)
        
        # set rpo to 0 in resultdir/data/metrics.csv
        metrics_csv = os.path.join(resultdir, 'data', 'metrics.csv')
        if not os.path.exists(metrics_csv):
            continue
        with open(metrics_csv, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        with open(metrics_csv, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
            writer.writeheader()
            for row in rows:
                row['rpo'] = 0
                writer.writerow(row)
    # 避免继续遍历子目录的子目录
    break