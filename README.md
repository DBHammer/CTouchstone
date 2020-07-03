# TouchstoneToolChain

TouchstoneToolChain是一款数据库信息采集工具，负责收集数据库配置信息，生成符合[Touchtone](https://github.com/daseECNU/Touchstone)输入格式的配置文件，由**华东师范大学数据科学与工程学院(DaSE@ECNU)** 维护。

当前支持数据库：

+ TiDB 3.*
+ TiDB 4.*

## 简介

TouchstoneToolChain使用java编写，支持java 1.8+，方便在各种环境中执行数据库和负载信息采集任务。

### 采集信息简介

TouchstoneToolChain采集数据库Schema信息，数据分布信息和负载信息，用于执行[Touchtone](https://github.com/daseECNU/Touchstone)的生成任务。具体采集信息如下：

+ Schema信息 

  表名，表⼤⼩，主键列，外键参照关系，列名和列数据类型，或者直接给定建表语句和表大小。

+ 数据库统计信息

| 类型                          | 采集信息                                           |
| :---------------------------- | -------------------------------------------------- |
| 数值类型列（int，decimal）    | null比例，最大值，最小值,（int需给定非重复值个数） |
| 日期类型列（date，datetime）  | null比例，起始时间，终止时间                       |
| 字符串类型列（varchar，char） | null比例，平均长度，最大长度，非重复值个数。       |
| bool类型列（bool）            | null比例，在非null中，true的比例                   |

+ 查询计划信息

  Query语句（其中参数可为符号变量） + Query执行计划 + 查询树中每一个节点的输出记录数。查询树中每一个节点的输出记录数，指每个节点执行后中间结果集的行数，以TPC-H的Query3示例如下图。

  <img src="https://tva1.sinaimg.cn/large/006y8mN6ly1g8pjl7u1p6j30d40cu0t3.jpg" alt="TPC-H_Query-3" style="zoom: 67%;" />

### 输出配置文件简介

TouchstoneToolChain采集完成后，会生成3部分配置文件

1. schema.conf

   该配置文件包含[Touchtone](https://github.com/daseECNU/Touchstone)需要的schema结构信息和数据分布的统计信息，但需要注意三点

   1. 工具只会输出待分析的query涉及到的table信息，没有涉及到的table不会出现配置文件中
   2. 工具会尝试用query中的信息推测主外键信息，当推测到环形依赖或者部分主键依赖时，工具无法分析，原因在于[Touchtone](https://github.com/daseECNU/Touchstone)暂不支持这种schema结构
   3. 当执行跨库分析任务时，table表名完全从query中采集，如果指定了查询库，则只会在本库中执行统计分析任务。

2. constraintsChain.conf

   该配置文件包含touchstone需要的约束链统计信息，使用`## 文件名_序号`来辨识分析结果中的sql语句，对于未能成功分析的sql，会保留`## 文件名_序号`的标识，但不会输出信息到文件中，同时会在程序执行中警示错误，方便后续的手动排查。

3. sqls/filename_index.sql

   程序会在指定目录下创建sqls文件夹，输出模版化的查询语句，在[Touchstone](https://github.com/daseECNU/Touchstone)中提供了自动填充功能，可以将实例化后的新参数，填充到query模板中，query模版示例如下，[Touchstone](https://github.com/daseECNU/Touchstone)通过辨识`#num,num,num#`标志，用实例化后的参数进行替换。

```sql
select c_custkey, c_name
	, sum(l_extendedprice * (1 - l_discount)) as revenue
	, c_acctbal, n_name, c_address, c_phone, c_comment
from customer, orders, lineitem, nation
where c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= '#25,0,1#'
	and o_orderdate < '#25,1,1#'
	and l_returnflag = '#26,0,0#'
	and c_nationkey = n_nationkey
group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
order by revenue desc
limit 20;
```

​		但是sql中可能会遇到无法模版化的情况，此时该工具会将模版信息以注释的方式写在sql头部，请手动模版化。工具运行过程中会产生类似如下提示，请按照提示排查。

```
1.sql_0	获取成功
请注意1.sql_0中有参数出现多次，无法智能替换，请查看该sql输出，手动替换
```

​		示例如下，由于1.sql中存在两个n_name，而在查询计划中统一了这两个，因此分析时无法智能替换，手动替换这两个即可。

```sql
-- conflictArgs:n_name =:[#2,0,0#],
select ps_partkey, sum(ps_supplycost * ps_availqty) as value
from partsupp, supplier, nation
where ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'MOZAMBIQUE'
group by ps_partkey
having sum(ps_supplycost * ps_availqty) > (
	select sum(ps_supplycost * ps_availqty) * 0.0001000000
	from partsupp, supplier, nation
	where ps_suppkey = s_suppkey
		and s_nationkey = n_nationkey
		and n_name = 'MOZAMBIQUE'
)
order by value desc;
```

## 快速上手

TouchstoneToolChain使用maven配置，git clone本项目后，可以如下命令编译生成可执行文件,生成路径位于`./target/TouchstoneToolchain-${version}.jar`。

```shell
mvn clean install
```

编译成功后，使用如下命令使用导入配置文件之后即可运行。

```shell
java -jar ./target/TouchstoneToolchain-${version}.jar CONFIG_PATH/config.conf
```

工具配置文件采用json格式，下面是一份配置文件示例。

```json
{
    "databaseIp": "localhost",
    "databaseName": "tpch",
    "databasePort": "4000",
    "databasePwd": "",
    "databaseUser": "root",
    "dataSource": "tidb",
    "databaseVersion": "3.1.0",
    "sqlsDirectory": "conf/sqls",
    "resultDirectory": "touchstoneconf",
    "loadDirectory": "load",
    "dumpDirectory": "dump",
    "skipNodeThreshold": 2000,
    "typeConvert": {
        "INTEGER": [
            "int"
        ],
        "DATETIME": [
            "date"
        ],
        "DECIMAL": [
            "decimal"
        ],
        "VARCHAR": [
            "varchar",
            "char"
        ],
        "BOOL": [
            "bool"
        ]
    },
    "tidbHttpPort": "10080",
    "tidbSelectArgs": {
        "LT": "<",
        "GT": ">",
        "LE": "<=",
        "GE": ">=",
        "EQ": "=",
        "NE": "<>",
        "LIKE": "like",
        "IN": "in",
        "ISNULL": "isnull",
        "OR": "or"
    }
}
```

配置项简介如下：

+ 数据库信息。按照配置文件顺序主要需要配置的字段有，待采集数据库的ip，数据库name，数据库端口，密码和用户名。

+ 数据源与数据库版本。配置用于采集信息的数据库类型和版本，如上的tidb v3.1.0

+ 文件夹路径

  1. 需要分析查询计划的sql文件夹路径，文件夹内待分析的文件必须以sql结尾，程序会尝试分析每个sql文件内的所有sql语句，单个sql文件内允许包含多组语句，但必须是select语句，其他类型语句未测试。
  2. 结果输出文件夹路径，程序会向指定路径输出touchstone的配置文件和模版化的sql。
  3. 持久化和加载文件夹路径，若没有dumpDirectory选项，就不会持久化；若没有loadDirectory选项，就不会加载

+ 容忍分析阈值。低于该阈值的节点在出错的情况下，会跳过该节点

+ 类型转化配置

  由于数据库中可能会有很多的属性，但是touchstone处理时，会按照常用类型处理，这里定义了可处理的5种类型，如果数据库中出现了未定义的类型，在配置文件中增加即可，比如bigint，smallint等等。

+ stats http端口 

  用于从http端口采集tidb的schema统计信息

+ select参数配置

  由于在查询计划中，tidb会把condition条件做映射转换，而touchstone使用常见的参数作为输入，因此这个配置文件中定义了如何找到对应的select参数，一般不需更改

## 特殊情况下的行为

1. 跨库信息采集

   TouchstoneToolChain引入配置项crossMultiDatabase，当设置为true时，TouchstoneToolChain从所有的query中采集所有的表名，用于采集数据库Schema信息和数据分布信息，但此时需要保证query中写入了库名。当设置为false时，TouchstoneToolChain仅从配置的数据库中获取所需信息，此时query中不需写明数据库名。但无论是否跨库采集，[Touchstone](https://github.com/daseECNU/Touchstone)在计算时一律当作同一库内的表来处理。

2. 数据基数约束为0

   在特殊情况下，查询计划某个算子的输出行数可能为0，进而导致后续计算的过滤比例除0，产生非法值。此时，TouchstoneToolChain会使用NaN标记，通知[Touchstone](https://github.com/daseECNU/Touchstone)在计算时，进行特殊处理。

3. 外键列非整型

   由于在一些生产环境中，存在使用非整形数据作为外键round操作后进行join的操作，在处理此类型时，约束链的输出会看待为整型的外键，去除round标识符，但是[Touchstone](https://github.com/daseECNU/Touchstone)在计算时会识别到外键列为非整形，从而进行处理。

4. 数据库中未显式配置主外键

   由于在一些生产环境中，并未在表上显式配置外键依赖，因此TouchstoneToolChain会尝试从query中推测主外键，推测依据基于基数和表大小。基数大的表会作为主键表，基数相同时，表小的表会作为主键表。

## 已知问题

- [ ] 暂且不支持UNION等集合操作
- [ ] 当查询计划中出现isnull算子时，暂时无法判定来自数据库查询计划自动生成还是查询语句写入，需要手动配置。

## 联系我们

研发团队：华东师范大学数据科学与工程学院 DBHammer数据库项目组。

该工作的主要成员：张蓉 教授，王清帅 在读博士，游舒泓，连薛超。

地址：上海市普陀区中山北路3663号。

邮政编码：200062。

联系邮箱：qswang@stu.ecnu.edu.cn。

对于文档内容和实现源码有任何疑问，可通过发起issue或发送邮件与我们联系，我们收到后将尽快给您反馈。
