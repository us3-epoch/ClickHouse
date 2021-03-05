clickhouse是一个用于OLAP的开源列式数据库，Yandex开发。

## 主要功能：

* 真正的列式数据库。 没有任何内容与值一起存储。例如，支持常量长度值，以避免将它们的长度“ number”存储在值的旁边。
* 线性可扩展性。 可以通过添加服务器来扩展集群。
* 容错性。 系统是一个分片集群，其中每个分片都是一组副本。ClickHouse使用异步多主复制。数据写入任何可用的副本，然后分发给所有剩余的副本。Zookeeper用于协调进程，但不涉及查询处理和执行。
* 能够存储和处理数PB的数据。
* SQL支持。 Clickhouse支持类似SQL的扩展语言，包括数组和嵌套数据结构、近似函数和URI函数，以及连接外部键值存储的可用性。
* 高性能。[13]
* 使用向量计算。数据不仅由列存储，而且由向量处理（一部分列）。这种方法可以实现高CPU性能。
* 支持采样和近似计算。
* 可以进行并行和分布式查询处理（包括JOIN）。
* 数据压缩。
* HDD优化。 该系统可以处理不适合内存的数据。

## 使用场景

* 它可以处理少量包含大量字段的表。
* 查询可以使用从数据库中提取的大量行，但只用一小部分字段。
* 查询相对较少(通常每台服务器大约100个RPS)。
* 对于简单的查询，允许大约50毫秒的延迟。
* 列值相当小，通常由数字和短字符串组成（例如每个URL，60字节）。
* 处理单个查询时需要高吞吐量（每台服务器每秒数十亿行）。
* 查询结果主要是过滤或聚合的。
* 数据更新使用简单的场景（通常只是批量处理，没有复杂的事务）。
* ClickHouse的一个常见情况是服务器日志分析。在将常规数据上传到ClickHouse之后（建议将数据每次1000条以上批量插入），就可以通过即时查询分析事件或监视服务的指标，如错误率、响应时间等。
 
ClickHouse还可以用作内部分析师的内部数据仓库。ClickHouse可以存储来自不同系统的数据（比如Hadoop或某些日志），分析人员可以使用这些数据构建内部指示板，或者为了业务目的执行实时分析。

## us3存储支持

### 源码下载

* 下载源码

```
git clone https://github.com/us3-epoch/ClickHouse
```

* 切到指定分支

```
git checkout us3_support
```

* 下载依赖的子模块

```
git submodule update --init --recursive
```

### 编译

clickhouse编译依赖gcc/llvm，cmake，ninja。如使用gcc，请确保版本在10及以上。可按[官方说明](https://clickhouse.tech/docs/en/development/build/)准备编译环境。

编译：

```bash
cd ClickHouse
mkdir build
cd build
cmake ..
ninja
```

*注：如使用gcc，请将`cmake ..`替换为`cmake -DENABLE_EMBEDDED_COMPILER=0 -DUSE_INTERNAL_LLVM_LIBRARY=0 -DWERROR=0 ..`

### 配置

如需使用us3作为后端存储，需在配置文件中增加disk配置。配置文件的详细设置请参考[官方链接](https://clickhouse.tech/docs/en/operations/server-configuration-parameters/settings/)

在配置文件的disks中增加如下配置：

```xml
        <disks>
            <your_name>
                <type>us3</type>
                <endpoint>ufile.cn-north-02.ucloud.cn</endpoint>
                <bucket>your-bucket</bucket>
                <access_key>***************</access_key>
                <secret_key>***************</secret_key>
                <prefix>/clickhouse/</prefix>
            </your_name>
        </disks>
```

policies中增加如下配置:

```xml
        <policies>
            <your_name>
                <volumes>
                    <main>
                        <disk>testdiskname</disk>
                    </main>
                </volumes>
            </your_name>
        </policies>
```

创建表时增加如下语句

```sql
SETTINGS your_setting,
storage_policy = 'your-policy-name';
```

即可创建使用us3作为存储后端的表。

可通过如下命令查看策略是否创建成功。

```
clickhouse-client

select * from system.storage_policies
```

输出如下（省略部分内容）：

```
┌─policy_name─┬─volume_name─┬─volume_priority─┬─disks────────────┬─volume_type─┬─max_data_part_size─┬─move_factor─┐
│ default     │ default     │               1 │ ['default']      │ JBOD        │                  0 │           0 │
│ testpolicy  │ main        │               1 │ ['testdiskname'] │ JBOD        │                  0 │         0.1 │
└─────────────┴─────────────┴─────────────────┴──────────────────┴─────────────┴────────────────────┴─────────────┘
```

## 性能差距

相比使用本地存储，性能上会有一定的损耗，时延大约是本地的8~9倍左右。
