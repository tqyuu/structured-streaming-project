# 题目一实现思路（prompt.md）

> 实时词频统计系统 · 基于 Socket 源的 Structured Streaming

---

## 1. 任务原文

> 场景：基于 Socket 源实现实时英文语句词频统计。
>
> 1. 编写 Structured Streaming 程序，从 `localhost:9999` 端口读取文本数据流，统计每个单词的出现频率，要求每 10 秒触发一次计算，使用 Complete 输出模式。
> 2. 修改程序，只统计长度为 3-5 个字母的单词，并将结果输出到控制台。
> 3. 使用 File 接收器将处理结果以 Parquet 格式保存到本地目录 `/tmp/wordcount_output`，并设置检查点目录。

---

## 2. 需求拆解：把自然语言翻译成 API

Structured Streaming 的核心模型是 **"流 = 一张不断追加的无界表"**。因此写流式程序就是三步：定义输入表（`readStream`）→ 对表做查询（DataFrame API）→ 定义输出（`writeStream`）。

| 需求 | 对应 API | 作用 |
| --- | --- | --- |
| 从 `localhost:9999` 读文本流 | `readStream.format("socket").option("host").option("port").load()` | 得到流式 DataFrame，schema 为 `value`（+ Spark 3.x 的 `timestamp`） |
| 一行拆成多个单词 | `split(col("value"), " ")` + `explode(...)` | 数组拍平成多行，一行句子变多行单词 |
| 统计单词频率 | `groupBy("word").count()` | 有状态聚合，维护每个词的累计次数 |
| 每 10 秒计算一次 | `trigger(processingTime="10 seconds")` | 定时触发微批，而非来一条算一条 |
| Complete 输出模式 | `outputMode("complete")` | 每次输出**全量最新**结果 |
| 只保留 3-5 字母单词 | `filter(length(col("word")).between(3, 5))` | 无状态过滤，放在聚合之前 |
| 保存 Parquet + 检查点 | `format("parquet")` + `option("path")` + `option("checkpointLocation")` | 文件接收器落盘，检查点保证故障恢复 |

---

## 3. 整体实现逻辑

```
nc -lk 9999 / mock_data_server.py
        │  TCP 长连接，一行一句
        ▼
socket source（value: string）
        │  split + explode
        ▼
words（word: string, event_time: timestamp）
        │
        ├── 问题1/2：filter(3~5) → groupBy(word).count()
        │        └── console sink + complete + trigger(10s)
        │
        └── 问题3：filter(3~5) + withWatermark(20s) + window(10s) 聚合
                 └── parquet sink + append + checkpoint
```

代码结构（[wordcount.py](wordcount.py)）：

| 函数 | 职责 |
| --- | --- |
| `build_spark()` | 创建 `SparkSession`（本地教学环境用 `local[*]`） |
| `word_count_socket(mode, host, port, output_path, checkpoint_path, runtime)` | 总控：按 `mode` 走三条分支，组装查询并启动 |
| `main()` | argparse 入口，`--mode basic/filter/parquet` 切换三个问题 |

---

## 4. 关键决策与原因

### 4.1 为什么问题 1/2 用 Complete 模式

- Complete 每轮输出**完整词频表**，新数据的影响直接体现在下一次全量结果中，**无需 watermark**，语义最简单；
- 代价是聚合状态（全量词表）常驻内存、永不清理，只适合演示/调试，不适合大流量生产。

### 4.2 为什么问题 3 不能"把 console 换成 parquet"就完事

这是本题唯一真正的坑，两个限制叠加：

1. **Parquet 文件写死后不可修改** → File Sink 只支持 `append` 输出模式，`complete` 会直接报错；
2. **append + 流式聚合必须带 watermark**，否则 Spark 抛：
   `Append output mode not supported when there are streaming aggregations ... without watermark`

原因是 append 要求"写出去的行永远正确"。词频会不断变化，Spark 必须能判定"某条结果以后不会再变"才能追加。判定手段就是 **事件时间窗口 + watermark**：

- 用 `window(event_time, "10 seconds")` 把无限流切成有限窗口，窗口内结果可定型；
- 用 `withWatermark("event_time", "20 seconds")` 声明"迟到超过 20 秒的数据不再等待"，水位线越过窗口结束时间后，该窗口结果被追加输出、状态被清理。

因此问题 3 的统计口径变为 **"每 10 秒窗口内各单词出现次数"**，输出列为 `window_start / window_end / word / count`，同时保留问题 2 的 3-5 字母过滤。

### 4.3 Spark 3.x / 4.x 的 socket 源 schema 差异

- Spark 3.x 的 socket 源自带 `timestamp` 列（数据到达时刻），可直接当事件时间；
- Spark 4.x 移除了该列，schema 只有 `value`。

代码采用运行时判断：`"timestamp" in lines.columns` 时用该列，否则用 `current_timestamp()` 兜底，保证两个版本都能运行。

### 4.4 空串过滤

`split(value, " ")` 在出现连续空格时会产生空字符串，聚合前用 `filter(col("word") != "")` 去掉，避免空词进入结果。

---

## 5. 运行方式

```bash
# 终端 1：数据源（Linux/Mac 用 nc；Windows 用自带模拟器）
nc -lk 9999                     # 手工敲英文句子，每回车一行
python mock_data_server.py      # 或：自动每秒发一句英文（道 Windows 无 nc）

# 终端 2：三种模式
python wordcount.py --mode basic                              # 问题1
python wordcount.py --mode filter                             # 问题2
python wordcount.py --mode parquet                            # 问题3
python wordcount.py --mode parquet --output D:/tmp/wordcount_output \
                                    --checkpoint D:/tmp/wordcount_checkpoint   # Windows 路径
```

`--runtime N` 可让作业运行 N 秒后自动优雅停止（演示/测试用）。

---

## 6. 验证要点

| 问题 | 观察点 |
| --- | --- |
| 问题1 | 控制台每 10 秒出现一个 `Batch`，输出为**全量词频表**（旧词计数累加、新词出现） |
| 问题2 | 表内只剩长度 3-5 的单词（如 `spark`、`data`），`is`/`we`/`the`/`technology` 等被过滤 |
| 问题3 | 输出目录按批生成 parquet 文件，回读可见 `window_start/window_end/word/count`；检查点目录包含 `offsets/commits/state` |

真实运行结果见 [result.md](result.md)。

---

## 7. 已知局限（可作为面试"项目不足"的答案）

1. socket 源**不可重放、无容错**，仅适合教学；生产应替换为 Kafka/Flume；
2. `event_time` 是"数据到达时间"而非业务事件时间，真实场景应在消息体内携带时间戳；
3. 未做标点剥离与大小写归一化（可加 `lower` + `regexp_replace`）；
4. 本地 `local[*]` 单机运行，未做并行度/状态后端（RocksDB）调优。
