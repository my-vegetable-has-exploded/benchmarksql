import yaml

# # 定义故障类型及其参数
# FAULT_TYPES = {
#     "fail": {"template": "fail.yaml", "params": {}},
#     "io_fault": {"template": "io_fault.yaml", "params": {"percent": [100, 80, 50, 20, 10]}},
#     "net_delay": {"template": "net_delay.yaml", "params": {"latency": ["1ms", "4ms", "8ms", "16ms", "32ms"]}},
#     "net_loss": {"template": "net_loss.yaml", "params": {"loss": ["5", "10", "15", "20"]}},
#     "cpu_stress": {"template": "cpu_stress.yaml", "params": {"load": [50, 100]}},
# }

FAULT_TYPES = {
    "fail": {"template": "fail.yaml", "params": {}},
    "io_fault": {"template": "io_fault.yaml", "params": {}},
    "net_delay": {"template": "net_delay.yaml", "params": {"latency": ["32ms"]}},
    "net_loss": {"template": "net_loss.yaml", "params": {"loss": ["100"]}},
    "cpu_stress": {"template": "cpu_stress.yaml", "params": {"load": [100]}},
}

# 定义故障位置生成函数
def generate_injectips(zone_type, role, pod_count):
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

# 过滤异常情况
def is_abnormal(zone_type, role, pod_count, fault_type, fault_params):
    # # 过滤掉对compute角色设置zone的情况
    # tmp abnormal
    if role == "compute" and (pod_count == 1 or fault_type == "io_fault"):
        return True

    # # 过滤掉对所有compute节点注入高网络延迟的情况
    # if role == "compute" and pod_count == 0 and zone_type == "":  # 只对storage角色的所有节点进行过滤
    #     if fault_type == "io_fault":
    #         percent_threshold = 50
    #         if fault_params.get("percent", 20) >= percent_threshold:
    #             return True
    #     if fault_type == "net_delay":
    #         # 提取数字部分进行比较
    #         current_latency = int(''.join(filter(str.isdigit, fault_params.get("latency", "1ms"))))
    #         threshold_latency = 16
    #         if current_latency > threshold_latency:
    #             return True
    #     if fault_type == "net_loss":
    #         loss_threshold = 10  # 网络丢包率阈值（单位：%）
    #         if int(fault_params.get("loss", 0)) >= loss_threshold:
    #             return True

    
    # 过滤掉对所有storage节点注入高比例IO故障或高网络延迟的情况
    if role == "storage" and pod_count == 0 and zone_type == "":  # 只对storage角色的所有节点进行过滤
        if fault_type == "io_fault":
            percent_threshold = 50
            if fault_params.get("percent", 20) >= percent_threshold:
                return True
        if fault_type == "net_delay":
            # 提取数字部分进行比较
            current_latency = int(''.join(filter(str.isdigit, fault_params.get("latency", "1ms"))))
            threshold_latency = 16
            if current_latency > threshold_latency:
                return True
        if fault_type == "net_loss":
            loss_threshold = 10  # 网络丢包率阈值（单位：%）
            if int(fault_params.get("loss", 0)) >= loss_threshold:
                return True
    # 过滤掉让所有存储和计算节点失效的情况
    if pod_count == 0 and fault_type == "fail" and zone_type == "": # 只对所有节点进行过滤
        return True
    return False

# 生成配置文件
def generate_config_file(zone_type, role, pod_count, fault_type, duration, fault_params):
    injectips = generate_injectips(zone_type, role, pod_count)
    fault_config = FAULT_TYPES[fault_type]
    config = {
        "template": fault_config["template"],
        "injectips": injectips,
        "duration": f"{duration}s"
    }

    # 将io故障路径设置为对应role的路径， 如storage角色设置为storage.volumnPath
    if fault_type == "io_fault":
        config["volumePath"] = "$" + role + ".volumePath"

    if fault_type == "cpu_stress":
        config["workers"] = 20

    if fault_type == "fail":
        config["process"] = "$process" 
    
    # 添加故障类型特定的参数
    for param, value in fault_params.items():
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
    
    # 写入YAML文件
    with open(file_name, 'w') as file:
        yaml.dump(config, file, default_flow_style=False)
    
    print(f"Generated {file_name}")

# 示例：生成多个配置文件
if __name__ == "__main__":
    # 定义故障位置、角色、故障类型等
    # zone_types = ["leader", "follower", "random"]  # 添加空字符串表示不指定zone
    zone_types = ["leader", "random"]  # 添加空字符串表示不指定zone
    # roles = ["storage", "compute", "metadata"]
    roles = ["storage", "compute"]
    pod_counts = [0] # 0表示对所有节点注入故障，i表示对i个节点注入故障
    # fault_types = ["cpu_stress"]
    fault_types = ["fail", "io_fault", "net_delay", "net_loss", "cpu_stress"]
    duration = 120  # 持续时间，单位为秒

    # 生成所有可能的组合
    for zone_type in zone_types:
        for role in roles:
            for pod_count in pod_counts:
                for fault_type in fault_types:
                    fault_config = FAULT_TYPES[fault_type]
                    if not fault_config["params"]:  # 如果没有参数（如fail类型）
                        if not is_abnormal(zone_type, role, pod_count, fault_type, {}):
                            generate_config_file(zone_type, role, pod_count, fault_type, duration, {})
                    else:
                        # 为每个参数值生成一个配置文件
                        for param, values in fault_config["params"].items():
                            for value in values:
                                if not is_abnormal(zone_type, role, pod_count, fault_type, {param: value}):
                                    generate_config_file(zone_type, role, pod_count, fault_type, duration, {param: value})
