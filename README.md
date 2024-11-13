# 可靠性测试工具设计与实现

## 概述

可靠性测试工具希望自动化实现可靠性测试用例的执行， 通过对被测系统进行负载测试， 在负载测试时自动化注入用例描述的故障内容， 观测被测系统在故障注入后的表现和故障恢复情况， 根据在各种故障场景下被测数据库系统的表现， 综合评估被测数据库系统的可靠性。

架构图

如上图所示，可靠性测试工具主要由若干个模块组成，包括用例生成模块、调度器、故障注入模块、配置管理模块、指标收集模块。通过将工具模块化，以接口化的形式调用相关服务，可以很好地降低系统耦合性，提高系统的可扩展性和迁移性。 因为被测系统的不同部署形态和测试需求， 可靠性测试工具无法直接固定工作负载和故障注入内容， 所以可靠性用例库以模板形式定义可靠性用例。 可靠性用例库中模板首先需要结合配置文件中的具体参数进行实例化，主要实例化内容主要包括结合配置确定负载内容、 被测数据库系统连接信息、 故障注入范围和其它故障配置信息。 实例化后的用例由调度器调度执行， 调度器会对整个用例的执行进行调度控制， 包括负载的执行、 故障注入的执行、 故障注入时间的控制等。负载的执行由模拟应用层负责，模拟应用层中的执行线程通过JDBC等方式执行负载内容。故障注入模块负责将实例化后的故障内容注入到被测系统中， 通过混沌工程工具实现故障注入。

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

```
db=oceanbase
driver=com.mysql.cj.jdbc.Driver
application=Generic
conn=jdbc:mysql://133.133.135.56:30083/tpccdb?useUnicode=true&characterEncoding=utf-8&rewriteBatchedStatements=true&allowMultiQueries=true
user=root@test#obcluster
password=123@Test

# Scaling and timing configuration
warehouses=200
loadWorkers=4
monkeys=12
sutThreads=32
rampupMins=5
rampupSUTMins=2
rampupTerminalMins=2
runMins=10
keyingTimeMultiplier=0.1
thinkTimeMultiplier=0.1

paymentWeight=0.0
orderStatusWeight=0.0
deliveryWeight=0.0
stockLevelWeight=0.0
```

# BenchmarkSQL 属性文件

BenchmarkSQL 通过 Java 属性格式的文件进行配置。
这些文件中的参数控制加载哪个 JDBC 驱动程序、数据库类型、连接凭证、缩放等。

## 驱动程序和连接参数

| 参数       | 描述                                                         | 示例            |
| ---------- | ------------------------------------------------------------ | --------------- |
| `db`       | 数据库类型<br/>这是一个字符串，可以是 *firebird*、*mariadb*、*oracle*、*postgres*、*transact-sql* 或 *babelfish*。基于此参数，**Generic** 应用程序将处理 SQL 方言的一些差异。<br/>例如，处理 **FOR UPDATE**。MSSQL 只允许在游标中使用 SQL 标准语法，但支持在常规 **SELECT** 中使用提示 **WITH(UPDLOCK)**。 | postgres         |
| `driver`   | 要加载的 JDBC 驱动程序类名。                           | org.postgresql.Driver |
| `application` | 客户端应用程序类型<br/>BenchmarkSQL 支持所有数据库类型在 *Generic* 应用程序类型中，它使用 JDBC.PreparedStatement() 并在客户端仅实现业务逻辑。另外两个选择是 *PostgreSQLStoredProc* 和 *OracleStoredProc*，它们分别在 PL/pgSQL 或 PL/SQL 中实现业务逻辑的存储过程。BenchmarkSQL 客户端仍然生成所有屏幕输入，将其传输到数据库并接收所有屏幕输出。这些实现的主要好处是大大减少了应用程序和数据库之间的网络往返次数。 | Generic         |
| `conn`     | JDBC 连接 URI                                          | jdbc:postgresql://dbhost/benchmarksql1 |
| `user`     | 数据库用户名                                       | benchmarksql     |
| `password` | 数据库用户密码                                    | ChangeOn1nstall   |

## 缩放参数

| 参数                         | 描述                                                         | 示例   |
| ---------------------------- | ------------------------------------------------------------ | ------ |
| `warehouses`                 | 整体数据库大小缩放参数<br/>这不仅影响初始数据库的大小，还影响 BenchmarkSQL 将模拟多少终端。 | 2000  |
| `loadWorkers`                | 用于创建初始数据库内容的并行线程数。应基于数据库服务器的 CPU 和 IO 容量。 | 8      |
| `monkeys`                    | 用于处理终端输入和输出的猴子线程数。详情请参考 [架构文档](./TimedDriver.md)。 | 8      |
| `sutThreads`                 | 要启动的应用程序线程数。BenchmarkSQL 将创建这么多并行数据库连接来处理传入的请求。 | 80     |
| `maxDeliveryBGThreads`       | 任何时候允许处理 Delivery 交易背景部分的 sutThreads 的最大数量。 | 40     |
| `maxDeliveryBGPerWarehouse`  | 允许处理 Delivery 交易背景部分“同一仓库”的 sutThreads 的最大数量。 | 1      |

关于 Delivery 背景交易的说明：TPC-C 有这个“批量”交易，称为 Delivery。
它挑选每个 `District` 中一个 `Warehouse` 的最旧的尚未交付的 `Order` 并“交付”它。
这涉及到从 `New-Order` 表中选择 10 行 `FOR UPDATE`，更新相应的 10 个 Order 行以及平均 100 个 `Order-Line` 行等。

这个背景交易有一个非常宽松的 80 秒完成时间。
显然，选择那些属于同一个 `Warehouse` 的 `New-Order` 行，已经创建了一个锁定冲突，所以将每个仓库的 Delivery 交易的并发数量限制为 1 是一个自然的选择。
`maxDeliveryBGThreads` 参数是作为一个控制机制，以防止所有 SUT 线程都忙于处理这种交易类型，而那些响应时间要求更紧的交易在 SUT Fifo 队列中等待。

## 定时和测试持续时间参数

| 参数                         | 描述                                                         | 示例   |
| ---------------------------- | ------------------------------------------------------------ | ------ |
| `rampupMins`                 | 实际基准测量开始之前的延迟时间（分钟）。在这个延迟期间执行的任何事务在统计和报告中都被忽略。这应该大于 rampupTerminalMins 参数，以给数据库一些时间在全负载下平稳。 | 30     |
| `rampupSUTMins`              | SUT 线程（数据库连接）启动的持续时间（分钟）。这应该低于或等于 rampupTerminalMins。 | 15     |
| `rampupTerminalMins`         | 模拟终端启动的持续时间（分钟）。终端负责所有的按键和思考延迟，因此将终端的启动分散在一段时间内将导致事务负载逐渐增加，而不是在零秒内从 0-100 的冲击。这将给数据库一些时间在热身缓存的同时跟上负载，而不是在客户端应用程序中积累大量的请求。 | 20     |
| `runMins`                    | 实际基准测量的持续时间（分钟）。这应该足够长，至少覆盖一个检查点。为了获得数据库行为的真实画面，建议持续几个小时甚至几天。 | 300    |
| `reportIntervalSecs`         | 测试驱动程序报告当前平均每分钟交易数量的间隔（秒）。 | 60     |
| `restartSUTThreadProbability` | SUT 线程在完成请求后安排启动替换并终止的概率。 | 0.001  |

## 吞吐量调整参数

| 参数                 | 描述                                                         | 示例    |
| -------------------- | ------------------------------------------------------------ | ------- |
| `keyingTimeMultiplier` | Keying 是模拟用户填写输入屏幕所需的固定秒数。这取决于交易类型。 | 0.1     |
| `thinkTimeMultiplier` | 类似于 Keying，Think 时间是模拟用户处理交易结果所需的时间，通过阅读输出屏幕。这是一个随机时间，每个交易类型都有一个定义的平均值。 | 0.1     |

## 交易混合参数

| 参数             | 描述                                                         | 示例    |
| ---------------- | ------------------------------------------------------------ | ------- |
| `paymentWeight`  | 支付交易的概率（百分比）。                                 | 43.1    |
| `orderStatusWeight` | 订单状态交易的概率（百分比）。                             | 4.1     |
| `stockLevelWeight` | 库存水平交易的概率（百分比）。                             | 4.1     |
| `deliveryWeight` | 交货交易的概率（百分比）。                                 | 4.1     |

newOrder事务的比例为100% - paymentWeight - orderStatusWeight - stockLevelWeight - deliveryWeight

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
- `pods`：k8s集群中被测数据库服务部署涉及到的pod,  格式为"标签名=标签值"，可以通过标签形式描述，使用`,`分隔
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
- `duration`：持续时间， 单位为毫秒

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