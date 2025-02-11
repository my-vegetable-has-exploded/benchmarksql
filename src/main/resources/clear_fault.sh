#!/bin/bash

# 远程服务器的用户名和IP地址
USER="wy"
HOST="133.133.135.56"

# 通过 SSH 连接到远程服务器并执行命令
ssh $USER@$HOST << 'EOF'
# 删除 chaos-testing 命名空间下的所有 iochaos
kubectl delete iochaos --all -n chaos-testing

# 删除 chaos-testing 命名空间下的所有 podchaos
kubectl delete podchaos --all -n chaos-testing

# 删除 chaos-testing 命名空间下的所有 networkchaos
kubectl delete networkchaos --all -n chaos-testing
EOF

# 脚本结束
echo "Commands executed on $HOST"