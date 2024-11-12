# 可靠性测试工具设计与实现

## 概述

可靠性测试工具希望自动化实现可靠性测试用例的执行， 通过对被测系统进行负载测试， 在负载测试时自动化注入用例描述的故障内容， 观测被测系统在故障注入后的表现和故障恢复情况， 根据在各种故障场景下被测数据库系统的表现， 综合评估被测数据库系统的可靠性。

架构图

如上图所示，可靠性测试工具主要由若干个模块组成，包括用例生成模块、调度器、故障注入模块、配置管理模块、指标收集模块。通过将工具模块化，以接口化的形式调用相关服务，可以很好地降低系统耦合性，提高系统的可扩展性和迁移性。 因为被测系统的不同部署形态和测试需求， 可靠性测试工具无法直接固定工作负载和故障注入内容， 所以可靠性用例库以模板形式定义可靠性用例。 可靠性用例库中模板首先需要结合配置文件中的具体参数进行实例化，实例化用例执行后，工具自动对被测 

## 部署

### 依赖

- Java 11
- Maven 3.8
- Python >= 3.9
- python库: flask numpy matplotlib jproperties

### 工具编译部署

```shell
mvn package -Dskiptest

python3 -m venv tool
source tool/bin/activate
pip3 install flask numpy matplotlib jproperties
cd target/run
./FlaskService/main.py & > tool.log 2>&1
```

设计内容

部署后通过`http://localhost:5000`访问服务

![ui](ui.png)

页面中按钮功能如下：

- RUN： 根据配置运行可靠性测试， 在填入配置后可执行
- BUILD： 根据配置初始化测试数据库， 建立测试表和索引， 写入初始化数据
- DESTROY： 删除测试数据库
- CANCEL: 取消当前测试
- REFRESH: 刷新页面

针对每次测试， 可以在Output中查看测试输出。 测试完成后， 可以在Result中查看测试结果。 Result中可以查看日志、结果报告等。

![结果报告](report.png)

### 被测系统部署

### 混沌工程工具部署

在k8s环境下部署混沌工程工具， 如ChaosBlade。 工具故障注入功能依赖于混沌工程工具， 在进行到故障点时， 工具通过ssh连接到k8s集群master节点并且传输故障文件， 通过ssh调用执行`kubectl apply -f`命令调用混沌工程工具应用故障配置文件来实现故障注入。

```
helm repo add chaosblade-io https://chaosblade-io.github.io/charts
helm install chaosblade chaosblade-io/chaosblade-operator --namespace chaosblade
```

## 配置

### 负载配置

负载配置同BenchmarkSQL， 待补充

### 故障配置

实例化故障时， 会根据配置文件中的故障配置， 对模板中参数进行填充， 最后生成可执行的故障文件。

故障配置和负载配置一起放入properties文件中。

```
k8scli:wy@133.133.135.56
namespace:oceanbase
iface:ens6f1
pods:ref-obzone=obcluster-1-zone1, ref-obzone=obcluster-1-zone2, ref-obzone=obcluster-1-zone3
leader:ref-obzone=obcluster-1-zone1
# port of db server
serverport:2883
faults:leader_fail.yaml
faulttime:3
```

以上述内容为例， 
- `k8scli`：k8s集群master的ssh连接信息， 部署机器需要配置ssh免密登陆
- `namespace`：k8s集群中部署被测数据库的namespace
- `iface`：部署机器的互联网卡名称
- `pods`：k8s集群中被测数据库服务部署涉及到的pod, 可以通过标签形式描述，使用`,`分隔
- `leader`：被测数据库的主节点pod标签
- `serverport`：被测数据库服务端口
- `faults`：需要实例化的故障模板文件
- `faulttime`：故障注入时间， 单位为分钟， 上述例子中表示故障在第3分钟结束注入

故障模板文件存储在`src/main/resources/FaultTemplates`目录下， 以yaml格式存储。 故障模板文件中需要引用混沌工程工具故障文件， 这部分存储在`src/main/resources/FaultTemplates/template`目录下, 以yaml格式存储。

```yaml
template: "fail_pod_by_labels.yaml"
# select the leader pod
podname: "$LEADER"
namespace: "$NAMESPACE"
duration: "0"
```

以主节点失效故障为例， `src/main/resources/FaultTemplates/leader_fail.yaml`描述了主节点失效故障的具体信息。

- `template`：使用的混沌工程工具故障文件
- `podname`：故障作用的pod名称， 通过`$`引用配置文件中的变量
- `namespace`：故障作用的pod所在namespace， 通过`$`引用配置文件中的变量
- `duration`：持续时间， 单位为分钟

结合配置文件， 该配置文件会填充混沌工程工具故障文件中`src/main/resources/FaultTemplates/template/fail_pod_by_labels.yaml`的变量， 生成可由混沌工程工具执行的故障文件。

原`src/main/resources/FaultTemplates/template/fail_pod_by_labels.yaml`文件如下, 其中`$PODNAME`和`$NAMESPACE`变量将被配置文件中的`leader`和`namespace`替换。

```yaml
apiVersion: chaosblade.io/v1alpha1
kind: ChaosBlade
metadata:
  name: fail-pod-by-labels
spec:
  experiments:
    - scope: pod
      target: pod
      action: fail
      desc: "inject fail image to select pod"
      matchers:
        - name: labels
          value:
            - "$PODNAME"
        - name: namespace
          value:
            - "$NAMESPACE"
        - name: evict-count
          value:
            - "1"
```

最终得到的故障文件如下：

```yaml
apiVersion: chaosblade.io/v1alpha1
kind: ChaosBlade
metadata:
  name: fail-pod-by-labels
spec:
  experiments:
	- scope: pod
	  target: pod
	  action: fail
	  desc: "inject fail image to select pod"
	  matchers:
		- name: labels
		  value:
			- "ref-obzone=obcluster-1-zone1"
		- name: namespace
		  value:
			- "oceanbase"
		- name: evict-count
		  value:
			- "1"
```

根据配置， 该故障会在第3分钟结束后注入， 作用于`ref-obzone=obcluster-1-zone1`的pod， 使其失效。

### 故障模板实例化方法与自定义故障方法



## 测试运行

### 测试流程

0. 部署被测系统、混沌工程工具和可靠性测试工具
1. 配置负载和故障配置
2. 点击BUILD按钮， 初始化测试数据库
3. 点击RUN按钮， 运行测试
4. 通过日志查看测试输出和结果报告查看测试结果

## 注意事项

当jsch连接出现报错时，首先检查可靠性测试工具所在机器能否免密连接到k8s集群master上, 如果出现com.jcraft.jsch.JSchException: invalid privatekey错误， 可以检查密钥开头是否为

```
-----BEGIN OPENSSH PRIVATE KEY----- 
```

jsch不支持这种密钥格式，需要使用命令`ssh-keygen -p -m pem -f ~/.ssh/id_rsa`将密钥转换为

```
-----BEGIN RSA PRIVATE KEY-----
```


TODO: 

1、对比数据库系统， 对结果进行分析
2、增加用例
3、完善文档， 设计部分
4、度量指标表格， 已完成、未完成， 故障数量： 对多少种故障具有容错能力、可以应对的故障数量，无法自动恢复