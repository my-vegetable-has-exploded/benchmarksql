#!/bin/bash

# API 地址
API_URL="http://133.133.135.56:5000/result_delete/"

# 循环删除 run_id 从 660 到 720 的数据
for run_id in {696..737}; do
    # 构造完整的 API 请求 URL
    url="${API_URL}?run_id=${run_id}"

    # 发送 DELETE 请求
    response=$(curl  "$url")

    # 检查请求是否成功
    if [[ "$response" -eq 200 ]]; then
        echo "Deleted run_id: $run_id (HTTP $response)"
    else
        echo "Failed to delete run_id: $run_id (HTTP $response)"
    fi
done