# 面试深挖问答手册（Structured Streaming 实时数据处理项目）

> 用法：不要背答案，先自己答一遍再看参考。每个主题按"追问链"组织——面试官通常沿一条链从浅挖到深。
> 标注 ⭐ 的是本项目直接用到、必考的点。

---

## 一、项目整体（开场必问）

### Q1. 用 1 分钟介绍一下这个项目 ⭐

> 参考话术：这个项目是用 Spark Structured Streaming 做的实时数据处理系统，包含两条链路。第一条是实时词频统计：从 Socket 端口读取文本流，每 10 秒一个微批做词频聚合，分别验证了 Complete 模式控制台输出、3-5 字母过滤、以及带 Watermark 的 Parquet 落盘三种方案。第二条是 Kafka 点击分析流水线：生产者每秒产生随机商品点击事件写入 input-topic，流式作业按 30 秒事件时间窗口统计每个商品的点击次数，以"商品ID:次数"格式写回 output-topic。通过这个项目我理解了流处理的三个核心问题：什么时候触发计算（Trigger）、结果怎么输出（Output Mode）、出了错怎么恢复（Checkpoint + 可重放 Source）。

### Q2. 为什么选 Structured Streaming 而不是老的 Spark Streaming（DStream）？

- DStream 基于 RDD API，批流两套 API，学习维护成本高；Structured Streaming 基于 DataFrame/SQL，**批流统一**，一套代码既跑流也跑批。
- Structured Streaming 能复用 **Catalyst 优化器和 Tungsten 执行引擎**，DStream 不能。
- 原生支持**事件时间（event time）和 Watermark**，DStream 只能按处理时间切 batch，处理乱序数据很别扭。
- 直接内置对接 Kafka/File 等 source/sink，支持 **exactly-once** 语义（配合 checkpoint）。

### Q3. 为什么不用 Flink / Kafka Streams？（可能被问）

- Flink 是真正的**逐条处理的纯流引擎**，延迟更低（毫秒级）、事件时间模型更成熟，是生产大数据流计算的主流；Spark SS 是微批（默认 ~200ms 起，Continuous 模式可到毫秒但仅限简单算子）。
- 选型逻辑：我们团队/课程生态已经是 Spark（批处理也在用 Spark），Structured Streaming 让批流共用一套引擎和 SQL 技能，工程成本最低；且本作业延迟要求是秒级，微批完全够用。
- Kafka Streams 是**库**不是集群框架，嵌在应用进程里，不涉及资源调度，适合轻量场景；需要独立集群资源管理和 SQL 能力时选 Spark/Flink。

---

## 二、编程模型与执行机制 ⭐（核心考察区）

### Q4. Structured Streaming 的核心编程思想是什么？

把流看成一张**不断追加数据的无界表（unbounded table）**。流式计算 = 在这张表上做**增量查询**：开发者像写批处理 DataFrame 一样写查询，引擎把它编译成在每个微批上**增量执行**的计划。`readStream` 得到 Input Table，`writeStream` 定义每批结果怎么写出去。

### Q5. 一个微批（micro-batch）从触发到完成经历了什么？（高频追问）

以 Kafka 源为例，每 30 秒（trigger）执行一轮：

1. **规划**：Source（KafkaSource）向 broker 查询每个分区当前最新 offset，与上批已处理的 offset 对比，确定本批要读的增量区间（可用 `maxOffsetsPerTrigger` 限流）；
2. **执行**：引擎生成增量执行计划，本批只处理 [prevOffset, latestOffset) 的数据，执行 filter/explode/window+count 聚合；有状态算子会把**中间状态**写入 state store；
3. **输出**：Sink 写出结果（文件/Kafka/内存），按 sink 特性决定是否幂等；
4. **提交**：把"这批处理到哪个 offset"写进 checkpoint 的 offsets/ 和 commits/ 文件，然后进入下一轮。

关键点：**先写结果、再记位点**的顺序配合幂等 sink，保证故障重启后重复处理也不会重复输出。

### Q6. 你代码里的 `explode(split(...))` 在流式里是什么执行过程？

`split` 把一行文本变成字符串数组（仍是 1 行），`explode`（= lateral view 的 DataFrame 写法）把数组**拍平成多行**，一行输入变 N 行单词，这是典型的 **n:1 → 1:n 无状态转换**。因为无状态，它不产生 state store 开销；随后 `groupBy` 才是有状态的。

### Q7. 流式查询里 Catalyst 优化还有效吗？

有效。Structured Streaming 中分析后的逻辑计划每个微批都会经过优化，比如谓词下推（filter 尽早执行）。但注意：**有些优化对流不安全**，例如把流式的 filter 推到有状态聚合之后/把聚合拆开可能改变语义，Spark 专门区分了"可优化"与"不允许"的规则（如禁止对 streaming aggregation 交换/合并操作过度改写）。

---

## 三、输出模式 Output Mode ⭐（本项目最大的设计点）

### Q8. Complete / Append / Update 三种模式的区别？

| 模式 | 每次触发输出什么 | 要求 | 典型 Sink |
|------|------------------|------|-----------|
| Complete | **全量**最新结果（整个聚合结果表） | 无需 watermark；全量状态常驻内存 | console（调试）、memory、Kafka |
| Append | 只追加**本批新"终结"的行**，写过的不再改 | 有聚合时**必须配 watermark** | 文件、Kafka、ForeachBatch |
| Update | 只输出**本批发生变化的行**（含更新） | 聚合必须有 watermark（Spark 2.4+ 之前约束不同；流式聚合+update 需 watermark） | console、Kafka、memory |

### Q9. ⭐ 为什么题目一问题 3（Parquet 文件落盘）必须改用 Append + Watermark？

这是本作业埋的最深的坑，一定要讲清楚：

1. **文件是不可变的**：Parquet 文件一旦写成就不能修改。如果允许 Complete 模式，第 N 批要"改写"之前所有历史文件（word 计数随时变化），文件系统语义上不支持 → **File Sink 只支持 Append**。
2. **Append + 无界聚合的矛盾**：聚合结果随时可能被新数据更新（比如某单词下一批又出现了）。Append 要求"一旦输出就永远正确"，那必须**确定某个窗口的结果不会再变**才能输出。
3. **Watermark 就是这个"确定不再变"的机制**：watermark = 当前观察到的最大事件时间 - 容忍延迟。Spark 保证不会再接收事件时间 < watermark 的数据，因此**结束时间 < watermark 的窗口已终结**，可以安全地追加输出。
4. 所以不加 watermark 直接 `outputMode("append") + groupBy` 会直接抛异常：`Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark`。

### Q10. 你题目一里 Complete 模式为什么不需要 Watermark？

Complete 模式每批输出**全量**结果，不存在"输出后不能再改"的约束，迟到数据下一批自然会体现在全量结果里，所以不需要 watermark，状态也**永不清理**（全量词表常驻）。副作用：状态随单词数线性增长，单词量极大时会 OOM —— 生产上 Complete 模式基本不用，这也是它只适合调试的原因。

---

## 四、Watermark 与事件时间窗口 ⭐

### Q11. Watermark 到底是什么？怎么算的？

全局单调递增的时间线：
**watermark = max(事件时间) - 延迟容忍阈值**（如我代码里 `withWatermark("event_time", "20 seconds")`）。
每批处理完，用本批最大事件时间更新 watermark。语义承诺：事件时间早于 watermark 的数据**不再参与聚合**。微批引擎中 water­mark 在批边界更新，批内不会变。

### Q12. 迟到数据（late data）怎么处理？

- 事件时间 < watermark 的数据：**默认直接丢弃**（聚合结果里看不到）；
- 工程上补救：① 调大延迟阈值（权衡状态大小和结果延迟）；② 把迟到数据写入死信队列/旁路存储，离线批处理补算（Lambda 架构思路）；③ 业务上接受丢弃并监控丢弃量。

### Q13. 窗口是怎么实现的？`window(event_time, "30s")` 展开成什么？

window 不是自定义 UDF，而是按**事件时间对齐**生成分组键：`window(start, duration)` 展开成 `start`、`end` 两列，把每条数据映射到形如 `10:00:00~10:00:30` 的窗口标识，本质仍是 `groupBy(窗口, key)` 的普通聚合。滑动窗口 `window(col, "30s", "10s")` 时一条数据会**复制到多个重叠窗口**。窗口聚合的中间状态按 (窗口, key) 存在 state store 里；配了 watermark 后，`end < watermark` 的窗口状态会被清理。

### Q14. 事件时间和处理时间你用的哪个？为什么？

用**事件时间**（Kafka 消息自带的 `timestamp` 列 / socket 源的 `timestamp` 列）。原因：处理时间是数据到达引擎的时刻，受消费延迟、微批堆积影响，统计窗口会"漂移"；事件时间反映业务发生时刻，配合 watermark 能正确处理乱序。限制：本项目的事件时间其实是"到达 broker 的时间"，更严谨的做法是业务消息体里携带点击发生时间。

---

## 五、Checkpoint 与容错语义 ⭐

### Q15. Checkpoint 目录里存了什么？

- `offsets/N`：第 N 批处理到的 source 位点（如 Kafka 各分区 offset）+ 当前批次元信息（含 watermark、触发时间）；
- `commits/N`：第 N 批已成功提交（sink 完成）的标记，与 offsets 配合判断"处理了但没提交"的批次需要重放；
- `state/`：有状态算子的状态数据（每个分区目录，内含 delta 文件与 snapshot）；
- `metadata`：作业 ID 等元信息。
（我这版代码是 Spark 3.x 经典架构；Spark 4.0 引入了单文件 state store v4 格式，概念一致。）

### Q16. Exactly-once 是怎么端到端保证的？⭐

三个条件拼起来的：

1. **可重放 Source**：Kafka 数据按 offset 持久存储在 broker，任意重读 → 流程在"读取"这步可重复；
2. **Checkpoint 位点管理**：重启后从 offsets/ 读出"上次提交位置"，从那里继续（未提交但已处理的批次会重放）；
3. **幂等/事务 Sink**：重放的批次不能产生重复的最终效果。File sink 靠"文件名由 batchId 决定 + 已存在即跳过"实现幂等；Kafka sink 本身是 at-least-once，需要业务侧用 key+offset 去重，或开 Kafka 事务（`kafka.commitCheckpointToKafka` 类配置，Spark 3.x 与 Kafka 事务集成有限）—— 所以严格说**我这条 Kafka→Kafka 链路是 at-least-once + 幂等设计空间**，下游消费按 (window, product_id) 去重即可等价 exactly-once。

> ⚠️ 加分细节：Socket 源**不能** exactly-once —— 数据读到内存就没了，broker 没有副本、没有位点可回放，故障重启只能"从当前继续"，中间数据永久丢失。这正是为什么生产必须用 Kafka 这类可重放源。

### Q17. 作业挂了怎么恢复？说出完整流程。

重启 `spark-submit`（相同 checkpointLocation），引擎：读 `offsets/` 最后一个批次号 → 对照 `commits/` 判断该批是否已提交 → 未提交则**重新执行该批**（source 按记录的 offset 重放）→ 有状态算子从 `state/` 恢复聚合状态 → 继续正常调度。

---

## 六、Kafka 集成细节 ⭐

### Q18. Structured Streaming 读 Kafka 得到什么 schema？

固定列：`key`、`value`（都是 **binary**，需 `cast("string")`）、`topic`、`partition`、`offset`、`timestamp`（消息写入 broker 时间）、`timestampType`，还有 headers（Spark 3.0+）。`value.cast("string")` 就是我在消费者里做的第一步解析。

### Q19. 消费位点存在哪？为什么没用 consumer group 的 auto-commit？

Structured Streaming **不用 Kafka 消费组的位点提交**，位点完全由自己的 checkpoint 管理（offsets/ 文件）。它会用一个内部生成的 group id（`spark-kafka-source-...`）连接 broker，但那只是为了让 Kafka 记住元数据，**不提交 offset**。好处：位点与处理进度、状态强一致，避免"auto-commit 造成的丢数据/重复消费"。

### Q20. `startingOffsets` 和重启后的行为？

`earliest`/`latest`/JSON 指定分区起始位点，**只在 checkpoint 不存在时生效一次**；一旦有 checkpoint，恢复一律以 checkpoint 位点为准。这就是为什么第一次跑我设 `latest`（只看新数据），断点重启不用改任何参数。

### Q21. 生产者为什么 `acks=all` + key=商品ID？

- `acks=all`：消息要等 ISR 全部副本落盘才确认，broker 单点宕机不丢数据（代价是延迟略高）；
- `key=product_id`：Kafka 按 key 哈希定分区 → **同一商品固定进同一分区**，分区内严格有序，同时让下游聚合的输入天然按商品局部性分布。

### Q22. Kafka sink 对输出 DataFrame 的要求？

必须有 `value` 列（string/binary），可选 `key`、`topic`、`partition`、`headers` 列。所以我用 `concat(product_id, ":", count)` 拼成 "商品ID:次数" 赋给 `value`，`product_id` 作 `key`。topic 也可以不在 option 里写死，而是每行带 topic 列实现**动态路由**。

---

## 七、状态管理（state store）

### Q23. 流式聚合的状态存在哪里？会不会一直膨胀？

存在 executor 内的 **State Store**（默认 `HDFSBackedStateStore`：内存 + 每次 commit 写本地磁盘 delta 文件，定期合并 snapshot）。膨胀控制两条路：
- 逻辑上：配 watermark，窗口 `end < watermark` 的状态自动清理；
- 物理上：状态过大换 **RocksDB State Store**（`spark.sql.streaming.stateStore.providerClass=...RocksDBStateStoreProvider`），把状态放 RocksDB（磁盘+内存缓存），内存占用小一个数量级，Spark 3.2+ 生产可用。

### Q24. 哪些算子有状态？你的作业里呢？

`filter/map/explode/union` 无状态，每批独立；`groupBy 聚合、stream-stream join、dropDuplicates、mapGroupsWithState` 有状态。我的词频统计（basic/filter 模式，Complete）是**全量状态**；parquet 模式和 Kafka 链路是**窗口状态 + watermark 清理**。

---

## 八、性能与生产化

### Q25. 消费不过来（积压）怎么办？

排查顺序：① 加 `maxOffsetsPerTrigger` **限流**防止单批 OOM（牺牲延迟保稳定）；② 看是否**数据倾斜**（某 key 巨大）：两阶段加盐聚合、或对热点 key 单独处理；③ 增加 Kafka 分区数 + 调大 `spark.sql.shuffle.partitions`（流式里 shuffle 分区数一旦跑起来不能变，因为状态按分区对应）；④ 换 RocksDB state store、调 trigger 间隔；⑤ 监控 `processedRowsPerSecond`、batch duration 与 trigger 的差值。

### Q26. 这个项目的延迟是多少？怎么测的？

微批语义下端到端延迟 ≈ trigger 间隔（10s/30s）+ 单批处理耗时（本地毫秒级）+ sink 提交耗时。观察 Spark UI 的 Streaming 标签页（或 `query.lastProgress`）里 batch duration 与 trigger 的关系：batch duration 稳定小于 trigger 间隔说明消费能力有余量。诚实说明：教学项目数据量小，真正的延迟压测是它明确的改进方向。

### Q27. 状态分区数 `spark.sql.shuffle.partitions` 为什么流式里不能中途改？

有状态算子的状态按"分区号 ↔ state store 实例"一一对应存在 checkpoint 里，改分区数会让恢复时状态对不上号。Spark 3.x 引入的 state store 分区数与 shuffle 分区数解耦（`spark.sql.streaming.statefulOperator.checkCorrectness` 相关），但仍要谨慎规划。这也提示：**上生产前要预估好并行度**。

---

## 九、架构对比与扩展（拔高题）

### Q28. 对比一下 DStream 和 Structured Streaming（一句话各自要害）

- DStream = **RDD 序列**，按处理时间切 micro-batch，API 老、无事件时间原生支持、exactly-once 要自己配（checkpoint + 幂等输出 + 存储事务）；
- Structured Streaming = **无界表上的增量查询**，DataFrame API、Catalyst 优化、事件时间/Watermark 一等公民、source/sink 语义内建。

### Q29. 如果数据量增长 100 倍，你怎么改造这个系统？（展示架构思维）

1. 采集层：Socket 换 Kafka 集群（多分区），上游用 Flink/SDK 直接打点；
2. 计算层：Spark 集群化，Kafka 分区 = shuffle 并行度；RocksDB state store；关键聚合改预聚合 + 二次聚合；
3. 存储层：Parquet 落 HDFS/S3（分区表），下游 OLAP（Doris/ClickHouse/StarRocks）做实时大盘；
4. 可靠性：checkpoint 放 HDFS/S3；Kafka 开事务或下游幂等去重；作业失败自动拉起（YARN/K8s restart policy）；
5. 监控：Prometheus + `query.status/lastProgress` 指标，迟到率/积压告警。

### Q30. 项目还有什么不足？（诚实反思，面试加分）

- Socket 源是教学源，不可重放 → 生产换 Kafka；
- 事件时间用的是到达时间，未从消息体解析业务时间；
- 未处理标点/大小写归一化；没有单元测试（可把拆词逻辑抽成纯函数用 pytest 测）；
- 未做容量规划和延迟压测；checkpoint 在本地盘（应放 HDFS）。

---

## 十、简历两条 bullet 的"防深挖"自检

| 简历表述 | 面试官可能追问 | 你的回答锚点 |
|----------|----------------|--------------|
| "10s 微批 + Complete 模式" | 为什么用 Complete？缺点？ | 全量输出无需 watermark；状态永不清理、不适合大词表 → 生产多用 Append |
| "File Sink 只支持 Append" | 为什么？ | Parquet 不可变；Append 输出必须保证"输出后不再变" → 需要 watermark 判定窗口终结（Q9 链条） |
| "Watermark 处理乱序" | watermark 公式？迟到数据去哪了？ | max(eventTime)-delay；丢弃，可旁路补算（Q11/Q12） |
| "Checkpoint 保证 exactly-once" | socket 源也行吗？Kafka sink 呢？ | socket 不可重放→最多 at-most-once；Kafka→Kafka 是 at-least-once + 下游幂等（Q16） |
| "30s 窗口统计点击" | 窗口怎么实现？状态何时清理？ | 窗口=分组键(start,end)；end<watermark 清理（Q13） |
