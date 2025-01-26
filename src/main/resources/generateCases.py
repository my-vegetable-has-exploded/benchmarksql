import yaml

# 定义故障类型及其参数
FAULT_TYPES = {
    "fail": {"template": "fail.yaml", "params": {}},
    "io_fault": {"template": "io_fault.yaml", "params": {"percent": [100, 80, 50, 20]}},
    "net_delay": {"template": "net_delay.yaml", "params": {"latency": ["1ms", "4ms", "16ms"]}},
    "net_loss": {"template": "net_loss.yaml", "params": {"loss": ["100", "80", "50", "20"]}},
}

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

# 生成配置文件
def generate_config_file(zone_type, role, pod_count, fault_type, duration, fault_params):
    injectpods = generate_injectpods(zone_type, role, pod_count)
    fault_config = FAULT_TYPES[fault_type]
    config = {
        "template": fault_config["template"],
        "injectpods": injectpods,
        "duration": f"{duration}s"
    }
    
    # 添加故障类型特定的参数
    for param, value in fault_params.items():
        config[param] = value
    
    # 生成文件名
    file_name_parts = []
    if zone_type:  # 如果指定了zone，才将zone描述加入文件名
        file_name_parts.append(zone_type)
        file_name_parts.append("zone")
    file_name_parts.extend([role, "all" if pod_count == 0 else "one", fault_type])
    
    # 将故障参数添加到文件名中
    for param, value in fault_params.items():
        file_name_parts.append(f"{param}_{value}")
    
    file_name = "_".join(file_name_parts) + ".yaml"
    
    # 写入YAML文件
    with open(file_name, 'w') as file:
        yaml.dump(config, file, default_flow_style=False)
    
    print(f"Generated {file_name}")

# 示例：生成多个配置文件
if __name__ == "__main__":
    # 定义故障位置、角色、故障类型等
    zone_types = ["leader", "follower", "random", ""]  # 添加空字符串表示不指定zone
    roles = ["storage", "compute"]
    pod_counts = [0, 1]
    fault_types = ["fail", "io_fault", "net_delay", "net_loss"]
    duration = 120  # 持续时间，单位为秒

    # 生成所有可能的组合
    for zone_type in zone_types:
        for role in roles:
            for pod_count in pod_counts:
                for fault_type in fault_types:
                    fault_config = FAULT_TYPES[fault_type]
                    if not fault_config["params"]:  # 如果没有参数（如fail类型）
                        generate_config_file(zone_type, role, pod_count, fault_type, duration, {})
                    else:
                        # 为每个参数值生成一个配置文件
                        for param, values in fault_config["params"].items():
                            for value in values:
                                generate_config_file(zone_type, role, pod_count, fault_type, duration, {param: value})