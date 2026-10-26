# 实时数据流处理系统（Spark Structured Streaming）

基于 **Spark Structured Streaming** 构建的实时数据处理项目，包含两个子系统：

| 子系统 | 数据源 | 处理逻辑 | 输出 |
|--------|--------|----------|------|
| 题目一：实时词频统计 | Socket（TCP 文本流） | 词频聚合 → 3-5 字母单词过滤 → 事件时间窗口聚合 | 控制台（Complete）/ Parquet 文件（Append） |
| 题目二：Kafka 点击分析流水线 | Kafka `input-topic` | 30 秒事件时间窗口内每商品点击次数聚合 | Kafka `output-topic`（`商品ID:次数`） |

## 项目架构

```
题目一（Socket 词频统计）

  nc -lk 9999 / mock_data_server.py          Spark Structured Streaming
  ┌──────────────────────┐   TCP:9999   ┌─────────────────────────────────┐
  │  英文文本行流          │ ───────────> │ socket source                   │
  │  "hello spark world" │              │   │ split/explode -> words      │
  └──────────────────────┘              │   │ filter(3<=len<=5)           │
                                        │   │ groupBy(window, word).count │
                                        │ sink: console(complete)         │
                                        │       parquet(append)+checkpoint│
                                        └─────────────────────────────────┘

题目二（Kafka 点击统计流水线）

  producer.py ──每秒1条──> Kafka input-topic ──> Structured Streaming ──30s窗口聚合──> Kafka output-topic
  "product_042"                                                              "product_042:17"
```

## 目录结构

```
structured-streaming-project/
├── README.md                          # 本文件
├── docs/
│   └── INTERVIEW_QA.md                # 面试深挖问答（重点复习材料）
├── problem1_socket_wordcount/
│   ├── prompt.md                      # 题目一实现思路（需求拆解/架构/关键决策）
│   ├── result.md                      # 题目一三个问题的真实运行结果
│   ├── wordcount.py                   # 题目一完整实现（问题1/2/3，--mode 切换）
│   └── mock_data_server.py            # Windows 下的模拟数据源（替代 nc -lk 9999）
└── problem2_kafka_pipeline/
    ├── prompt.md                      # 题目二实现思路
    ├── result.md                      # 题目二三个问题的真实运行结果
    ├── producer.py                    # 题目二-问题1：点击事件生产者
    └── consumer.py                    # 题目二-问题2/3：流式聚合 + Kafka Sink
```

## 快速开始

### 环境要求

- Python 3.8+，`pip install pyspark kafka-python`
- JDK 8/11/17（Spark 4.x 需 Java 17+）
- 题目二另需 Kafka 2.x/3.x（云平台已部署则跳过）
- **Windows 本地调试注意**：代码里的 `/tmp/...` 默认路径是 Linux 风格，本地跑请加参数改成 Windows 路径（如 `--output D:/tmp/wordcount_output`）

### 题目一：Socket 词频统计

```bash
# 终端1：造数据（二选一）
nc -lk 9999                        # Linux/Mac，手工输入句子
python mock_data_server.py         # Windows，自动每秒发一句英文

# 终端2：运行流式作业
python wordcount.py --mode basic     # 问题1：全量词频，10s触发，Complete模式
python wordcount.py --mode filter    # 问题2：只统计 3-5 字母单词
python wordcount.py --mode parquet   # 问题3：Parquet 落盘 + checkpoint
                                     #   结果查看：/tmp/wordcount_output 下的 parquet 文件
```

### 题目二：Kafka 点击统计流水线

```bash
# 0. 准备（云平台已做则跳过）
kafka-topics.sh --create --topic input-topic  --bootstrap-server localhost:9092
kafka-topics.sh --create --topic output-topic --bootstrap-server localhost:9092

# 终端1：生产者（每秒发一个 product_xxx）
python producer.py

# 终端2：流式消费者（30s 窗口聚合，写回 output-topic）
python consumer.py --debug          # --debug 同时打印到控制台，方便演示

# 终端3：查看最终结果
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic output-topic
```

## 核心技术点（也是面试考察点）

| # | 技术点 | 本项目的落点 |
|---|--------|--------------|
| 1 | 流=无界表的编程模型 | `readStream` 读入 Input Table，每个微批增量查询 |
| 2 | 微批触发器 Trigger | `trigger(processingTime="10s"/"30s")` 控制延迟与吞吐的权衡 |
| 3 | 三种输出模式 | Complete（控制台全量）/ Append（文件、Kafka 增量）/ Update |
| 4 | Watermark 水位线 | Append 模式聚合必须配 watermark；同时决定迟到容忍度和状态清理 |
| 5 | 事件时间窗口 | `window(event_time, "30 seconds")`，区别于处理时间 |
| 6 | 检查点容错 | offsets/commits/state 三类数据支撑故障恢复与 exactly-once |
| 7 | Kafka 位点与容错语义 | `startingOffsets` + checkpoint 接管位点；默认 at-least-once，失败重试可能重复，需下游幂等去重 |
| 8 | 有状态/无状态算子 | filter/explode 无状态；groupBy+window 有状态（依赖 state store） |

## 简历描述参考

> **实时数据流处理系统**（Spark Structured Streaming / Kafka）　个人项目
>
> - 基于 Structured Streaming 构建两条实时链路：Socket 词频统计与 Kafka 商品点击分析流水线，端到端延迟控制在秒级；
> - 使用 `explode`+`groupBy` 窗口聚合实现流式 WordCount 与 30s 滚动窗口点击计数，针对 File Sink 只支持 Append 模式的限制，引入 Watermark + 事件时间窗口解决无界聚合无法追加输出的问题；
> - 通过 Checkpoint（offsets/commits/state）实现故障自动恢复，并基于 Kafka 可重放特性分析 at-least-once 到 exactly-once 的差距与幂等补偿方案；对比验证 Complete/Append/Update 三种输出模式的适用场景。

（面试前务必先读 `docs/INTERVIEW_QA.md`，每一条 bullet 都可能被追问。）

## 已知限制（面试被问"项目有什么不足"时用）

1. Socket 源仅用于教学演示：**不可重放、无容错**，生产上应替换为 Kafka/Flume 等；
2. 事件时间用的是"数据到达时间"（socket/kafka 的 timestamp），并非业务时间；真实场景需在消息体中携带事件时间字段；
3. 词频统计未处理标点符号和大小写归一化（可加 `lower`+`regexp_replace` 扩展）；
4. 单机 `local[*]` 运行，未做分区数/并行度调优与状态后端（RocksDB）压测。
