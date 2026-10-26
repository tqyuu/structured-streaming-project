# 题目二实现思路（prompt.md）

> Kafka 实时数据处理流水线 · Structured Streaming + Kafka

---

## 1. 任务原文

> 场景：基于 Kafka 源构建完整的数据处理流水线。
>
> 1. 编写生产者程序，每秒生成一个随机商品 ID（格式：`product_xxx`）并发送到 Kafka 的 `input-topic` 主题。
> 2. 编写 Structured Streaming 消费者程序，从 `input-topic` 读取数据，统计每 30 秒内每个商品的点击次数。
> 3. 将统计结果写入 Kafka 的 `output-topic` 主题，格式为 `商品ID:次数`。

---

## 2. 需求拆解：三段式流水线

```
producer.py                     Kafka                     consumer.py（Structured Streaming）
每秒 1 条随机商品 ID   ──>   input-topic   ──>   读流 → 解析 → 30s 窗口聚合 → 写流   ──>   output-topic
"product_042"                                  （Spark 消费）                      "product_042:17"
```

| 需求 | 对应 API | 说明 |
| --- | --- | --- |
| 每秒生成商品 ID 并发到 Kafka | `KafkaProducer.send(topic, key, value)` + `time.sleep(1)` | 无限循环模拟点击事件流 |
| 从 `input-topic` 读取 | `readStream.format("kafka").option("subscribe", ...)` | Kafka 源 schema：`key/value` 为 bytes，另带 `topic/partition/offset/timestamp` 元数据 |
| 二进制 value 转字符串 | `col("value").cast("string")` | Kafka 消息体是字节流，必须显式解码 |
| 每 30 秒每商品点击次数 | `withWatermark` + `groupBy(window(event_time, "30 seconds"), product_id).count()` | 事件时间滚动窗口 + 有状态聚合 |
| 结果写回 Kafka | `writeStream.format("kafka").option("topic", ...)` | Kafka Sink 要求 DataFrame 含 `value`（必需）和 `key`（可选）列 |
| 输出 `商品ID:次数` | `concat(product_id, lit(":"), count.cast("string"))` | 拼装消息体 |
| 故障恢复 | `option("checkpointLocation", ...)` | 位点、提交记录与聚合状态持久化 |

---

## 3. 关键设计决策

### 3.1 生产者把商品 ID 同时作为 message key

```python
producer.send(topic, key=product_id, value=product_id)
```

Kafka **只保证分区内有序**。以商品 ID 作 key，Kafka 用 `hash(key) % partitions` 选分区，因此同一商品的消息固定进入同一分区，保证单商品事件的相对顺序。如果 key 为空，消息会轮询分散到各分区。

同时设置 `acks="all"`（等待所有 ISR 副本确认）与 `retries=3`，避免 broker 抖动导致丢消息。

### 3.2 事件时间取自 Kafka 消息元数据

```python
col("timestamp").alias("event_time")
```

Kafka 源自带 `timestamp` 列（消息到达 broker 的时间），直接作为事件时间，无需在消息体内额外解析时间字段。这是简化处理——严格的事件时间应由生产端写入业务时间戳。

### 3.3 30 秒窗口 + 10 秒水位线

```python
clicks.withWatermark("event_time", "10 seconds")
      .groupBy(window(col("event_time"), "30 seconds"), col("product_id"))
      .count()
```

- `window(..., "30 seconds")` 是**滚动窗口（tumbling）**：窗口不重叠、首尾相接，每个事件只属于一个窗口；
- `withWatermark("event_time", "10 seconds")` 容忍 10 秒乱序：水位线 = 当前见到的最大事件时间 − 10s，越过某窗口结束时间后，该窗口结果定型并可输出、旧状态被清理；
- 没有 watermark，`append` 模式的窗口聚合会被 Spark 拒绝执行。

### 3.4 输出必须构造成 Kafka Sink 要求的列

Spark 的 Kafka Sink 校验结果 schema：必须存在 `value` 列（string 或 binary），`key` 列可选。因此聚合后要重组：

```python
counts.select(
    col("product_id").alias("key"),
    concat(col("product_id"), lit(":"), col("count").cast("string")).alias("value"),
)
```

这里刻意**保留 `product_id` 作为输出 key**：下游按商品消费时可以继续利用分区有序性。

### 3.5 append 模式 + checkpoint

- `outputMode("append")`：窗口结果只在"定型"后追加输出，天然契合 Kafka 追加写的语义；
- `trigger(processingTime="30 seconds")`：与窗口粒度对齐，每 30 秒提交一个微批；
- `checkpointLocation` 保存 `offsets/`（已消费位点）、`commits/`（已提交批次）、`state/`（聚合中间状态），重启后自动续跑；
- `startingOffsets="latest"` 只影响**首次启动**（从最新位点开始），之后一律由 checkpoint 接管，避免重复消费历史数据。

### 3.6 `--debug` 双输出

同一份聚合结果同时挂两个 query：一个写 Kafka（append），一个打印控制台（update）。`update` 只输出本批有变化的行，适合演示时观察窗口结果如何逐批推进。

---

## 4. 代码结构

| 文件 | 关键函数 | 职责 |
| --- | --- | --- |
| [producer.py](producer.py) | `kafka_producer(bootstrap, topic, interval, max_products)` | 建立 KafkaProducer，循环发送 `product_xxx`，Ctrl+C 优雅关闭 |
| [consumer.py](consumer.py) | `kafka_consumer(bootstrap, input_topic, output_topic, checkpoint, window, debug)` | 读 Kafka → 解析 → 水位线窗口聚合 → 写 Kafka（可选同时打印控制台） |

---

## 5. 运行方式

```bash
# 0. 创建主题（云平台已创建可跳过；本地开 auto.create.topics.enable 也会自动建）
kafka-topics.sh --create --topic input-topic  --bootstrap-server localhost:9092
kafka-topics.sh --create --topic output-topic --bootstrap-server localhost:9092

# 终端1：生产者，每秒一条
python producer.py --interval 1 --max-products 100

# 终端2：消费者，30 秒窗口聚合，写回 output-topic
python consumer.py --debug

# 终端3：查看输出主题
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic output-topic --from-beginning
```

---

## 6. 验证要点

| 问题 | 观察点 |
| --- | --- |
| 问题1 | 生产者控制台每秒打印 `已发送: product_xxx`；`input-topic` 的 offset 持续增长 |
| 问题2 | 消费者控制台每 30 秒出现一批 `window / product_id / count`，count 为该窗口内点击数 |
| 问题3 | `output-topic` 中每条消息形如 `product_042:17`，且同一窗口内各商品计数之和等于窗口内消息总数 |

真实运行结果见 [result.md](result.md)。
