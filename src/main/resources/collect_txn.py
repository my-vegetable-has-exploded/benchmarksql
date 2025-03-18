import os
import re
import csv
from collections import defaultdict

# 定义目录路径
base_dir = 'service_data'

# 用于存储不同alphaTxn对应的recovery end值
alphaTxn_data = defaultdict(list)

# 遍历service_data目录
for dir_name in os.listdir(base_dir):
	# 352 - 447
    if re.match(r'result_\d{6}', dir_name) and dir_name >= 'result_000280':
        dir_path = os.path.join(base_dir, dir_name)
        console_log_path = os.path.join(dir_path, 'console.log')
        
        alphaTxn = None

        # 检查console.log文件是否存在
        if os.path.isfile(console_log_path):
            with open(console_log_path, 'r') as file:
                content = file.read()
                
                # 提取alphaTxn的值
                alphaTxn_match = re.search(r'skew\.alphaTxn=([\d.]+)', content)
                if alphaTxn_match:
                    alphaTxn = float(alphaTxn_match.group(1))
                    
                    # # 提取所有recovery end的值
                    # recovery_end_matches = re.findall(r'recovery end \{\} (\d+)', content)
                    # if recovery_end_matches:
                    #     last_recovery_end = int(recovery_end_matches[-1])  # 取最后一个值
                    #     if last_recovery_end > -1:  # 确保值有效
                    #         alphaTxn_data[alphaTxn].append(last_recovery_end)
        else:
            continue
        
        if alphaTxn is None:
            continue

        # 提取所有recovery end的值，从metrics.csv中recovery_time_factor字段
        metrics_csv_path = os.path.join(dir_path, 'data/metrics.csv')
        if os.path.isfile(metrics_csv_path):
            with open(metrics_csv_path, 'r') as file:
                rows = csv.DictReader(file)
                for row in rows:
                    recovery_time_factor = int(row['recovery_time_factor'])
                    alphaTxn_data[alphaTxn].append(recovery_time_factor)
        
                    print(f'result: {dir_name}, alphaTxn: {alphaTxn}, Recovery Ends: {recovery_time_factor}')
        

print('alphaTxn_data:', alphaTxn_data)

# 按alphaTxn大小排序输出
for alphaTxn in sorted(alphaTxn_data.keys()):
    recovery_ends = alphaTxn_data[alphaTxn]
    adjusted_recovery_ends = sorted(recovery_ends)[:]  # 去掉最小和最大值
    # adjusted_recovery_ends = sorted(recovery_ends)[1:-1]  # 去掉最小和最大值
    average_recovery_end = sum(adjusted_recovery_ends) / len(adjusted_recovery_ends)
    print(f'alphaTxn: {alphaTxn}, Recovery Ends: {recovery_ends}, Average Recovery End: {average_recovery_end}')