#!/usr/bin/env bash

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

kubectl delete StressChaos --all -n chaos-testing

kubectl delete -f /tmp/fail_then_fail_another_zone.yaml
EOF

# 脚本结束
echo "Commands executed on $HOST"

if [ $# -ne 1 ] ; then
    echo "usage: $(basename $0) PROPS_FILE" >&2
    exit 2
fi

SEQ_FILE="./.jTPCC_run_seq.dat"
if [ ! -f "${SEQ_FILE}" ] ; then
    echo "0" > "${SEQ_FILE}"
fi
SEQ=$(expr $(cat "${SEQ_FILE}") + 1) || exit 1
echo "${SEQ}" > "${SEQ_FILE}"

source ./funcs.sh $1

setCP || exit 1

myOPTS="-Dprop=$1 -DrunID=${SEQ}"
myOPTS="${myOPTS} -Djava.security.egd=file:/dev/./urandom -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"

java -cp "$myCP" $myOPTS com.github.pgsqlio.benchmarksql.jtpcc.jTPCC &
PID=$!
while true ; do
    kill -0 $PID 2>/dev/null || break
    sleep 1
done
