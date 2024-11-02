#! /bin/bash

# benchmarksql
source bin/activate 
cd target/run
./FlaskService/main.py & > benchmarksql.log 2>&1

# chaosblade
# nohup java --add-opens java.base/java.lang=ALL-UNNAMED -Duser.timezone=Asia/Shanghai -jar chaosblade-box-1.0.4.jar --spring.datasource.url="jdbc:mysql://127.0.0.1:3306/chaosblade?characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai" --spring.datasource.username=root --spring.datasource.password=123@Test --chaos.server.domain=127.0.0.1:7001 --chaos.function.sync.type=ALL  --chaos.prometheus.api=http://127.0.0.1:9090/api/v1/query_range > chaosblade-box.log 2>&1 &