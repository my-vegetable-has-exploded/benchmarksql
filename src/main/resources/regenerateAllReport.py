import os
import subprocess

# 定义 service_data 目录的路径
service_data_dir = './service_data'

# 遍历 service_data 目录下的所有子目录
for root, dirs, files in os.walk(service_data_dir):
    for dir_name in dirs:
        # 构建 resultdir 的路径
        resultdir = os.path.join(root, dir_name)
        
        # 构建要执行的命令
        command = ['./generateReport.py', '-t', 'report_simple.html', '--resultdir', resultdir]
        
        # 打印正在执行的命令
        print(f"Executing: {' '.join(command)}")
        
        # 执行命令
        try:
            subprocess.run(command, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error executing command: {e}")
    
    # 避免继续遍历子目录的子目录
    break