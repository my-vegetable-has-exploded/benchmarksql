import os
import re
from collections import defaultdict

# 定义目录路径
base_dir = 'service_data'

# 用于存储不同distributedRatio和distributedNodes对应的recovery_factor值
data = defaultdict(lambda: defaultdict(list))

# 遍历service_data目录
for dir_name in os.listdir(base_dir):
    # 448 - 
	# 551
    if re.match(r'result_\d{6}', dir_name) and dir_name >= 'result_000551':
        dir_path = os.path.join(base_dir, dir_name)
        console_log_path = os.path.join(dir_path, 'console.log')
        
        # 检查console.log文件是否存在
        if os.path.isfile(console_log_path):
            with open(console_log_path, 'r') as file:
                content = file.read()
                
                # 提取distributedRatio和distributedNodes的值
                distributedRatio_match = re.search(r'distributedRatio=([\d.]+)', content)
                distributedNodes_match = re.search(r'distributedNodes=(\d+)', content)
                
                if distributedRatio_match and distributedNodes_match:
                    distributedRatio = float(distributedRatio_match.group(1))  # 转换为float
                    distributedNodes = int(distributedNodes_match.group(1))
                    
                    # 提取recovery_factor的值
                    recovery_factor_match = re.search(r"'recovery_factor': ([\d.]+)", content)
                    if recovery_factor_match:
                        recovery_factor = float(recovery_factor_match.group(1))
                        
                        # 存储recovery_factor值
                        data[distributedNodes][distributedRatio].append(recovery_factor)

# 按distributedNodes和distributedRatio排序输出
for distributedNodes in sorted(data.keys()):
    for distributedRatio in sorted(data[distributedNodes].keys()):
        recovery_factors = sorted(data[distributedNodes][distributedRatio])[1:-1]
        if len(recovery_factors) == 0:
            continue
        average_recovery_factor = sum(recovery_factors) / len(recovery_factors)
        print(f'distributedNodes: {distributedNodes}, distributedRatio: {distributedRatio}, '
              f'Recovery Factors: {recovery_factors}, Average Recovery Factor: {average_recovery_factor}')